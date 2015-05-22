package org.icij.extract;

import java.lang.Runtime;

import java.util.Queue;

import java.util.logging.Level;
import java.util.logging.Logger;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.charset.Charset;

import java.io.IOException;
import java.io.FileNotFoundException;

import org.apache.tika.parser.ParsingReader;
import org.apache.tika.exception.TikaException;
import org.apache.tika.exception.EncryptedDocumentException;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public class Consumer {
	public static final int DEFAULT_THREADS = Runtime.getRuntime().availableProcessors();
	public static final long DEFAULT_TIMEOUT = 500L;
	public static final TimeUnit DEFAULT_TIMEOUT_UNIT = TimeUnit.MILLISECONDS;

	private final Logger logger;
	private final Queue queue;
	private final Spewer spewer;
	private final ThreadPoolExecutor executor;

	private Reporter reporter = null;

	private Charset outputEncoding;
	private String ocrLanguage;
	private boolean detectLanguage;
	private long pollTimeout = DEFAULT_TIMEOUT;
	private TimeUnit pollTimeoutUnit = DEFAULT_TIMEOUT_UNIT;

	private Runnable whenDrained = null;

	public Consumer(Logger logger, Queue queue, Spewer spewer, int threads) {
		this.logger = logger;
		this.queue = queue;
		this.spewer = spewer;
		this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(threads);
	}

	public Consumer(Logger logger, Spewer spewer, int threads) {
		this(logger, null, spewer, threads);
	}

	public void setPollTimeout(long timeout, TimeUnit unit) {
		pollTimeout = timeout;
		pollTimeoutUnit = unit;
	}

	public void setPollTimeout(String duration) throws IllegalArgumentException {
		TimeUnit unit = TimeUnit.MILLISECONDS;
		final long timeout;
		final Matcher matcher = Pattern.compile("^(\\d+)(h|m|s|ms)?$").matcher(duration);

		if (!matcher.find()) {
			throw new IllegalArgumentException("Invalid timeout string: " + duration + ".");
		}

		timeout = Long.parseLong(matcher.group(1));

		if (2 == matcher.groupCount()) {
			switch (matcher.group(2)) {
			case "h":
				unit = TimeUnit.HOURS;
				break;
			case "m":
				unit = TimeUnit.MINUTES;
				break;
			case "s":
				unit = TimeUnit.SECONDS;
				break;
			case "ms":
				unit = TimeUnit.MILLISECONDS;
				break;
			}
		}

		setPollTimeout(timeout, unit);
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

	public void setReporter(Reporter reporter) {
		this.reporter = reporter;
	}

	public void consume() {
		executor.execute(new Runnable() {

			@Override
			public void run() {
				Path file = (Path) queue.poll();

				if (null == file && pollTimeout > 0L) {
					logger.info("Polling the queue, waiting up to " + pollTimeout + " " + pollTimeoutUnit + ".");

					// TODO: Use wait/notify instead.
					try {
						Thread.sleep(TimeUnit.MILLISECONDS.convert(pollTimeout, pollTimeoutUnit));
					} catch (InterruptedException e) {
						return;
					}

					file = (Path) queue.poll();
				}

				// Shut down the executor if the queue is empty.
				if (null == file) {
					whenDrained();
					return;
				}

				maybeExtract(file);
				consume();
			}
		});
	}

	public void consume(final Path file) {
		executor.execute(new Runnable() {

			@Override
			public void run() {
				maybeExtract(file);
			}
		});
	}

	public void whenDrained(Runnable whenDrained) {
		this.whenDrained = whenDrained;
	}

	private void whenDrained() {
		if (null != whenDrained) {
			whenDrained.run();
		}
	}

	public void shutdown() {
		executor.shutdown();
	}

	private void maybeExtract(Path file) {

		// Check status in reporter. Skip if good.
		if (null != reporter && reporter.succeeded(file)) {
			return;
		}

		final int status = extract(file);

		// Save status to registry and start a new job.
		if (null != reporter) {
			reporter.save(file, status);
		}
	}

	private int extract(Path file) {
		logger.info("Processing: " + file + ".");

		final Extractor extractor = new Extractor(logger, file);

		if (null != ocrLanguage) {
			extractor.setOcrLanguage(ocrLanguage);
		}

		if (null != outputEncoding) {
			extractor.setOutputEncoding(outputEncoding);
		}

		ParsingReader reader = null;
		int status = Reporter.SUCCEEDED;

		try {
			reader = extractor.extract(file);
		} catch (FileNotFoundException e) {
			logger.log(Level.WARNING, "File not found: " + file + ". Skipping.", e);
			status = Reporter.NOT_FOUND;
		} catch (IOException e) {
			logger.log(Level.WARNING, "The document stream could not be read: " + file + ". Skipping.", e);
			status = Reporter.NOT_READ;
		} catch (EncryptedDocumentException e) {
			logger.log(Level.WARNING, "Skipping encrypted file: " + file + ".", e);
			status = Reporter.NOT_DECRYPTED;
		} catch (TikaException e) {
			logger.log(Level.WARNING, "The document could not be parsed: " + file + ". Skipping.", e);
			status = Reporter.NOT_PARSED;
		}

		if (Reporter.SUCCEEDED != status) {
			return status;
		}

		// TODO:
		// Check if file mime is supported by OCR parser.
		// If so, get the first bufferred 4k from the reader and run language detection.
		// Switch the language on the extractor to the detected language.
		// Run again.

		logger.info("Outputting: " + file + ".");

		try {
			spewer.write(file, reader, outputEncoding);
		} catch (IOException e) {
			logger.log(Level.SEVERE, "The extracted text could not be outputted: " + file + ".", e);
			status = Reporter.NOT_SAVED;
		}

		try {
			reader.close();
		} catch (IOException e) {
			logger.log(Level.SEVERE, "Error while closing file: " + file + ".", e);
		}

		return status;
	}
}
