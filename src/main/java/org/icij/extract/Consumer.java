package org.icij.extract;

import java.util.logging.Logger;

import java.nio.file.Path;
import java.nio.charset.Charset;

import java.io.IOException;

import org.apache.tika.parser.ParsingReader;
import org.apache.tika.exception.TikaException;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public class Consumer {
	private final Logger logger;

	private final Spewer spewer;

	private Charset outputEncoding;
	private String ocrLanguage;
	private boolean detectLanguage;

	public Consumer(Logger logger, Spewer spewer) {
		this.logger = logger;
		this.spewer = spewer;
	}

	public void setOutputEncoding(Charset outputEncoding) {
		this.outputEncoding = outputEncoding;
	}

	public void setOutputEncoding(String outputEncoding) {
		setOutputEncoding(Charset.forName(outputEncoding));
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

		// TODO:
		// Check if file mime is supported by OCR parser.
		// If so, get the first bufferred 4k from the reader and run language detection.
		// Switch the language on the extractor to the detected language.
		// Run again.

		logger.info("Outputting: " + file + ".");

		try {
			spewer.write(file, reader, outputEncoding);
		} catch (IOException e) {
			throw e;
		} finally {
			reader.close();
		}

		return file;
	}
}
