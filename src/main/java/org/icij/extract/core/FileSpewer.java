package org.icij.extract.core;

import java.util.Iterator;

import java.util.logging.Logger;

import java.io.File;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import java.nio.file.Path;
import java.nio.charset.Charset;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.TaggedOutputStream;

/**
 * Writes the text output from a {@link ParsingReader} to the filesystem.
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

	public void write(Path file, ParsingReader reader, Charset outputEncoding) throws IOException, SpewerException {
		Path outputFile = filterOutputPath(outputDirectory.resolve(file));

		// Add the output extension.
		if (null != outputExtension) {
			outputFile = outputFile.getFileSystem().getPath(outputFile.toString() + "." + outputExtension);
		}

		logger.info("Outputting to file: " + outputFile + ".");

		// Make the required directories.
		final Path outputParent = outputFile.getParent();
		if (null != outputParent) {
			final File outputFileParent = outputParent.toFile();
			final boolean madeDirs = outputFileParent.mkdirs();

			// The `mkdirs` method will return false if the path already exists.
			if (false == madeDirs && !outputFileParent.isDirectory()) {
				throw new SpewerException("Unable to make directories for file: " + outputFile + ".");
			}
		}

		final TaggedOutputStream outputStream = new TaggedOutputStream(new FileOutputStream(outputFile.toFile()));

		try {
			IOUtils.copy(reader, outputStream, outputEncoding);
		} catch (IOException e) {
			if (outputStream.isCauseOf(e)) {
				throw new SpewerException("Error writing output to file: " + outputFile + ".", e);
			} else {
				throw e;
			}
		} finally {
			outputStream.close();
		}
	}
}
