package org.icij.spewer;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.commons.io.TaggedIOException;
import org.apache.commons.io.output.TaggedOutputStream;
import org.apache.tika.metadata.Metadata;
import org.icij.extract.document.TikaDocument;
import org.icij.extract.extractor.Extractor;
import org.icij.task.Options;
import org.icij.task.annotation.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Writes the text or HTML output from a {@link Reader} to the filesystem.
 * Metadata is written to a JSON file.
 *
 * @since 1.0.0-beta
 */
@Option(name = "outputDirectory", description = "Directory to output extracted text. Defaults to the " +
		"current directory.", parameter = "path")
@Option(name = "outputFormat", description = "Set the output format. Either \"text\" or \"HTML\". " +
		"Defaults to text output.", parameter = "type")
public class FileSpewer extends Spewer implements Serializable {

	private static final Logger logger = LoggerFactory.getLogger(FileSpewer.class);

	private static final long serialVersionUID = -6541331052292803766L;

	private Path outputDirectory = Paths.get(".");
	private String outputExtension = "txt";

	public FileSpewer(final FieldNames fields) {
		super(fields);
	}

	@Override
	public FileSpewer configure(final Options<String> options) {
		super.configure(options);

		final Extractor.OutputFormat outputFormat = options.get("outputFormat").parse()
				.asEnum(Extractor.OutputFormat::parse).orElse(null);

		if (null != outputFormat && outputFormat.equals(Extractor.OutputFormat.HTML)) {
			outputExtension = "html";
		}

		options.get("outputDirectory").parse().asPath().ifPresent(this::setOutputDirectory);
		return this;
	}

	public void setOutputDirectory(final Path outputDirectory) {
		this.outputDirectory = outputDirectory;
	}

	@Override
	public void write(final TikaDocument tikaDocument) throws IOException {
		final Path outputPath = getOutputPath(tikaDocument);

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
				throw new TaggedIOException(new IOException(String.format("Unable to make directories for file: \"%s\".",
						contentsOutputPath)), this);
			}
		}

		TaggedOutputStream tagged = null;

		// #copy buffers the input so there's no need to use an output buffer.
		try (final OutputStream output = Files.newOutputStream(contentsOutputPath)) {
			tagged = new TaggedOutputStream(output);
			copy(tikaDocument.getReader(), tagged);
		} catch (IOException e) {
			if (null != tagged && tagged.isCauseOf(e)) {
				throw new TaggedIOException(new IOException(String.format("Error writing output to file: \"%s\".",
						contentsOutputPath), e), this);
			} else {
				throw e;
			}
		}

		if (outputMetadata) {
			writeMetadata(tikaDocument);
		}
	}

	private void writeMetadata(final TikaDocument tikaDocument) throws IOException {
		final Metadata metadata = tikaDocument.getMetadata();
		Path outputPath = getOutputPath(tikaDocument);
		outputPath = outputPath.getFileSystem().getPath(outputPath.toString() + ".json");

		logger.info(String.format("Outputting metadata to file: \"%s\".", outputPath));

		try (final JsonGenerator jsonGenerator = new JsonFactory().createGenerator(outputPath.toFile(),
				JsonEncoding.UTF8)) {
			jsonGenerator.useDefaultPrettyPrinter();
			jsonGenerator.writeStartObject();

			new MetadataTransformer(metadata, fields).transform(jsonGenerator::writeStringField, (name, values)-> {
				jsonGenerator.writeArrayFieldStart(name);
				jsonGenerator.writeStartArray();

				for (String value: values) {
					jsonGenerator.writeString(value);
				}
			});

			jsonGenerator.writeEndObject();
			jsonGenerator.writeRaw('\n');
		} catch (IOException e) {
			throw new TaggedIOException(new IOException("Unable to output JSON."), this);
		}
	}

	private Path getOutputPath(final TikaDocument tikaDocument) {
		final Path path = tikaDocument.getPath();

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

	@Override
	protected void writeDocument(TikaDocument doc, TikaDocument parent, TikaDocument root, int level) {
		throw new UnsupportedOperationException("Not implemented.");
	}
}
