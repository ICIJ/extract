package org.icij.extract.spewer;

import java.io.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.io.output.TaggedOutputStream;

import org.apache.tika.metadata.Metadata;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonEncoding;

import org.icij.extract.document.Document;
import org.icij.extract.extractor.Extractor;
import org.icij.task.Options;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * Writes the text or HTML output from a {@link Reader} to the filesystem.
 * Metadata is written to a JSON file.
 *
 * @since 1.0.0-beta
 */
public class FileSpewer extends Spewer {

	private static final Logger logger = LoggerFactory.getLogger(FileSpewer.class);

	private static final String DEFAULT_EXTENSION = "txt";

	private Path outputDirectory = Paths.get(".");
	private String outputExtension = DEFAULT_EXTENSION;

	public FileSpewer(final FieldNames fields) {
		super(fields);
	}

	@Override
	public FileSpewer configure(final Options<String> options) {
		super.configure(options);

		final Extractor.OutputFormat outputFormat = options.get("output-format").parse()
				.asEnum(Extractor.OutputFormat::parse).orElse(null);

		if (null != outputFormat && outputFormat.equals(Extractor.OutputFormat.HTML)) {
			setOutputExtension("html");
		}

		options.get("output-directory").parse().asPath().ifPresent(this::setOutputDirectory);
		return this;
	}

	public void setOutputDirectory(final Path outputDirectory) {
		this.outputDirectory = outputDirectory;
	}

	public Path getOutputDirectory() {
		return outputDirectory;
	}

	public String getOutputExtension() {
		return outputExtension;
	}

	public void setOutputExtension(final String outputExtension) {
		if (null == outputExtension || outputExtension.trim().isEmpty()) {
			this.outputExtension = null;
		} else {
			this.outputExtension = outputExtension.trim();
		}
	}

	@Override
	public void close() throws IOException {}

	@Override
	public void write(final Document document, final Reader reader) throws IOException {
		final Path outputPath = getOutputPath(document);

		// Add the output extension.
		Path contentsOutputPath;
		if (null != outputExtension) {
			contentsOutputPath = outputPath.getFileSystem().getPath(outputPath.toString() + "." + outputExtension);
		} else {
			contentsOutputPath = outputPath;
		}

		logger.info(String.format("Outputting to file: \"%s\".", contentsOutputPath));

		// Make the required directories.
		final Path outputParent = contentsOutputPath.getParent();
		if (null != outputParent) {
			final File outputFileParent = outputParent.toFile();
			final boolean madeDirs = outputFileParent.mkdirs();

			// The {@link File#mkdirs} method will return false if the path already exists.
			if (!madeDirs && !outputFileParent.isDirectory()) {
				throw new SpewerException(String.format("Unable to make directories for file: \"%s\".",
						contentsOutputPath));
			}
		}

		TaggedOutputStream tagged = null;

		// #copy buffers the input so there's no need to use an output buffer.
		try (final OutputStream output = Files.newOutputStream(contentsOutputPath)) {
			tagged = new TaggedOutputStream(output);
			copy(reader, tagged);
		} catch (IOException e) {
			if (null != tagged && tagged.isCauseOf(e)) {
				throw new SpewerException(String.format("Error writing output to file: \"%s\".", contentsOutputPath), e);
			} else {
				throw e;
			}
		}

		if (outputMetadata) {
			writeMetadata(document);
		}
	}

	@Override
	public void writeMetadata(final Document document) throws IOException {
		final Metadata metadata = document.getMetadata();
		Path outputPath = getOutputPath(document);
		outputPath = outputPath.getFileSystem().getPath(outputPath.toString() + ".json");

		logger.info(String.format("Outputting metadata to file: \"%s\".", outputPath));

		try (final JsonGenerator jsonGenerator = new JsonFactory().createGenerator(outputPath.toFile(),
				JsonEncoding.UTF8)) {
			jsonGenerator.useDefaultPrettyPrinter();
			jsonGenerator.writeStartObject();

			for (String name : metadata.names()) {
				String normalizedName = normalizeMetadataName(name);

				if (metadata.isMultiValued(name)) {
					jsonGenerator.writeArrayFieldStart(normalizedName);
					jsonGenerator.writeStartArray();

					for (String value: metadata.getValues(name)) {
						jsonGenerator.writeString(value);
					}
				} else {
					jsonGenerator.writeStringField(normalizedName, metadata.get(name));
				}
			}

			jsonGenerator.writeEndObject();
			jsonGenerator.writeRaw('\n');
		} catch (IOException e) {
			throw new SpewerException("Unable to output JSON.", e);
		}
	}

	private Path getOutputPath(final Document document) {
		final Path path = document.getPath();

		// Join the file path to the output directory path to parse the output path.
		// If the file path is absolute, the leading slash must be removed.
		if (null != outputDirectory) {
			if (path.isAbsolute()) {
				return outputDirectory.resolve(path.toString().substring(1));
			}

			return outputDirectory.resolve(path);
		}

		return path;
	}
}
