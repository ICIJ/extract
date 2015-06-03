package org.icij.extract.core;

import java.util.Iterator;

import java.util.logging.Logger;

import java.io.File;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import java.nio.file.Path;
import java.nio.charset.Charset;

import org.apache.tika.parser.ParsingReader;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.TaggedOutputStream;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public class FileSpewer extends Spewer {

	private final Path outputDirectory;
	private String outputExtension = "txt";

	public FileSpewer(Logger logger, Path outputDirectory) {
		super(logger);
		this.outputDirectory = outputDirectory;
	}

	public void setOutputExtension(String extension) {
		this.outputExtension = extension;
	}

	public void write(Path file, ParsingReader reader, Charset outputEncoding) throws IOException, SpewerException {
		int i = 0;
		Path outputFile = null;

		// Remove the common root of the file path.
		// If the file is at /path/to/some/random/file and the output directory is /path/to/output, then the resulting text is outputted to /path/to/output/some/random/file.
		if (outputDirectory.isAbsolute() && file.isAbsolute()) {
			final Iterator iterator = outputDirectory.iterator();

			while (iterator.hasNext()) {
				Object segment = iterator.next();

				if (!segment.equals(file.getName(i))) {
					break;
				}

				i++;
			}
		}

		if (i < 1) {
			i = 1;
		}

		// TODO: Support output-base option.
		outputFile = outputDirectory.resolve(file.subpath(i, file.getNameCount()));
		outputFile = outputFile.getFileSystem().getPath(outputFile.toString() + "." + outputExtension);

		logger.info("Outputting to file: " + outputFile + ".");

		// Make the required directories.
		final File outputFileParent = outputFile.getParent().toFile();
		final boolean madeDirs = outputFileParent.mkdirs();

		// The `mkdirs` method will return false if the path already exists.
		if (false == madeDirs && !outputFileParent.isDirectory()) {
			throw new SpewerException("Unable to make directories for file: " + outputFile + ".");
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
