package org.icij.extract;

import java.util.Iterator;
import java.util.logging.Logger;

import java.io.OutputStream;
import java.io.FileOutputStream;

import java.io.IOException;

import java.nio.file.Path;
import java.nio.charset.Charset;

import org.apache.tika.parser.ParsingReader;

import org.apache.commons.io.IOUtils;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public class FileSpewer extends Spewer {
	private Logger logger;

	private Path outputDirectory;

	public FileSpewer(Logger logger) {
		this.logger = logger;
	}

	public void setOutputDirectory(Path outputDirectory) {
		this.outputDirectory = outputDirectory;
	}

	public void write(Path file, ParsingReader reader, Charset outputEncoding) throws IOException {
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

		outputFile = outputDirectory.resolve(file.subpath(i, file.getNameCount()));

		if (outputFile.equals(file)) {
			throw new IllegalArgumentException("Output file cannot be the same as the input file.");
		}

		logger.info("Outputting to file: " + outputFile + ".");

		// Make the required directories.
		outputFile.getParent().toFile().mkdirs();

		final OutputStream outputStream = new FileOutputStream(outputFile.toFile());

		try {
			IOUtils.copy(reader, outputStream, outputEncoding);
		} catch (IOException e) {
			throw e;
		} finally {
			outputStream.close();
		}
	}
}
