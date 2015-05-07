package org.icij.extract;

import java.util.Iterator;

import java.util.logging.Level;
import java.util.logging.Logger;

import java.nio.file.Path;
import java.nio.charset.Charset;

import java.io.OutputStream;
import java.io.FileOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.io.IOUtils;

import org.apache.tika.parser.ParsingReader;
import org.apache.tika.exception.TikaException;
import org.apache.tika.exception.EncryptedDocumentException;

import org.xml.sax.SAXException;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public class Consumer {
	private Logger logger;

	private Path outputDirectory;
	private Charset outputEncoding;
	private String ocrLanguage;
	private boolean detectLanguage;

	public Consumer(Logger logger) {
		this.logger = logger;
	}

	public void setOutputEncoding(Charset outputEncoding) {
		this.outputEncoding = outputEncoding;
	}

	public void setOutputEncoding(String outputEncoding) {
		setOutputEncoding(Charset.forName(outputEncoding));
	}

	public void setOutputDirectory(Path outputDirectory) {
		this.outputDirectory = outputDirectory;
	}

	public void setOcrLanguage(String ocrLanguage) {
		this.ocrLanguage = ocrLanguage;
	}

	public void detectLanguageForOcr() {
		this.detectLanguage = true;
	}

	public Path consume(Path file) throws IOException, TikaException {
		logger.info("Processing: " + file + ".");

		final Extractor extractor = new Extractor(logger, file);

		if (null != ocrLanguage) {
			extractor.setOcrLanguage(ocrLanguage);
		}

		if (null != outputEncoding) {
			extractor.setOutputEncoding(outputEncoding);
		}

		final ParsingReader reader = extractor.extract(file);
		final Charset outputEncoding = extractor.getOutputEncoding();

		logger.info("Outputting: " + file + ".");

		try {
			if (null != outputDirectory) {
				save(file, reader, outputEncoding);
			} else {
				IOUtils.copy(reader, System.out, outputEncoding);
			}
		} catch (IOException e) {
			throw e;
		} finally {
			reader.close();
		}

		return file;
	}

	private void save(Path file, ParsingReader reader, Charset outputEncoding) throws IOException {
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
