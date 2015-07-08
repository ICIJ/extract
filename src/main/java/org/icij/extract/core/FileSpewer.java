package org.icij.extract.core;

import java.util.Iterator;

import java.util.logging.Logger;

import java.io.File;
import java.io.Reader;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import java.nio.file.Path;
import java.nio.charset.Charset;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.TaggedOutputStream;

import org.apache.tika.metadata.Metadata;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonEncoding;

/**
 * Writes the text or HTML output from a {@link Reader} to the filesystem.
 * Metadata is written to a JSON file.
 *
 * @since 1.0.0-beta
 */
public class FileSpewer extends Spewer {
	public static final String DEFAULT_EXTENSION = "txt";

	private final Path outputDirectory;
	private String outputExtension = DEFAULT_EXTENSION;

	public FileSpewer(Logger logger, Path outputDirectory) {
		super(logger);
		this.outputDirectory = outputDirectory;
	}

	public String getOutputExtension() {
		return outputExtension;
	}

	public void setOutputExtension(String outputExtension) {
		if (null == outputExtension || outputExtension.trim().isEmpty()) {
			this.outputExtension = null;
		} else {
			this.outputExtension = outputExtension.trim();
		}
	}

	public void write(final Path file, final Metadata metadata, final Reader reader,
		final Charset outputEncoding) throws IOException, SpewerException {

		// Join the file path to the output directory path to get the ouput path.
		// If the file path is absolute, the leading slash must be removed.
		Path baseOutputFile = filterOutputPath(file);
		if (baseOutputFile.isAbsolute()) {
			baseOutputFile = outputDirectory.resolve(baseOutputFile.toString().substring(1));
		} else {
			baseOutputFile = outputDirectory.resolve(baseOutputFile);
		}

		// Add the output extension.
		Path contentsOutputFile;
		if (null != outputExtension) {
			contentsOutputFile = baseOutputFile.getFileSystem().getPath(baseOutputFile
				.toString() + "." + outputExtension);
		} else {
			contentsOutputFile = baseOutputFile;
		}

		logger.info(String.format("Outputting to file: %s.", contentsOutputFile));

		// Make the required directories.
		final Path outputParent = contentsOutputFile.getParent();
		if (null != outputParent) {
			final File outputFileParent = outputParent.toFile();
			final boolean madeDirs = outputFileParent.mkdirs();

			// The `mkdirs` method will return false if the path already exists.
			if (false == madeDirs && !outputFileParent.isDirectory()) {
				throw new SpewerException(String.format("Unable to make directories for file: %s.", contentsOutputFile));
			}
		}

		TaggedOutputStream tagged = null;

		try (

			// IOUtils#copy buffers the input so there's no need to use an output buffer.
			final OutputStream output = new FileOutputStream(contentsOutputFile.toFile());
		) {
			tagged = new TaggedOutputStream(output);
			IOUtils.copy(reader, tagged, outputEncoding);
		} catch (IOException e) {
			if (null != tagged && tagged.isCauseOf(e)) {
				throw new SpewerException(String.format("Error writing output to file: %s.", contentsOutputFile), e);
			} else {
				throw e;
			}
		}

		if (outputMetadata) {
			writeMetadata(baseOutputFile.getFileSystem().getPath(baseOutputFile
				.toString() + ".json"), metadata);
		}
	}

	private void writeMetadata(final Path metaOutputFile, final Metadata metadata)
		throws IOException, SpewerException {
		logger.info(String.format("Outputting metadata to file: %s.", metaOutputFile));

		try (
			final JsonGenerator jsonGenerator = new JsonFactory().createGenerator(metaOutputFile.toFile(),
				JsonEncoding.UTF8);
		) {
			jsonGenerator.useDefaultPrettyPrinter();
			jsonGenerator.writeStartObject();

			for (String name : metadata.names()) {
				jsonGenerator.writeStringField(name, metadata.get(name));
			}

			jsonGenerator.writeEndObject();
			jsonGenerator.writeRaw('\n');
		} catch (IOException e) {
			throw new SpewerException("Unable to output JSON.", e);
		}
	}
}
