package org.icij.extract;

import java.lang.Runtime;

import java.util.Iterator;
import java.util.HashSet;
import java.util.Set;

import java.util.logging.Level;
import java.util.logging.Logger;

import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ExecutionException;

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
public abstract class Consumer {
	public static final int DEFAULT_THREADS = Runtime.getRuntime().availableProcessors();

	protected final Logger logger;
	protected final Spewer spewer;
	protected final ThreadPoolExecutor executor;

	protected Reporter reporter = null;

	private Charset outputEncoding;
	private String ocrLanguage;
	private boolean detectLanguage;

	protected int threads;

	protected Set<Future> futures = new HashSet<Future>();
	protected final Semaphore semaphore = new Semaphore(1);

	public Consumer(Logger logger, Spewer spewer, int threads) {
		this.logger = logger;
		this.spewer = spewer;
		this.threads = threads;
		this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(threads);
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

	public void shutdown() {
		logger.info("Shutting down consumer.");

		executor.shutdown();
	}

	public void consume(final Path file) {
		logger.info("Sending to thread pool; will queue if full (" + executor.getActiveCount() + " active): " + file + ".");

		futures.add(executor.submit(new Runnable() {

			@Override
			public void run() {
				lazilyExtract(file);

				// Garbage collect futures that have already completed.
				// Return immediately if no permit is available to avoid a deadlock with `await()`.
				if (!semaphore.tryAcquire()) {
					return;
				}

				final Iterator<Future> iterator = futures.iterator();

				while (iterator.hasNext()) {
					final Future future = iterator.next();

					if (future.isDone()) {
						iterator.remove();
					}
				}

				semaphore.release();
			}
		}));
	}

	public void finish() throws InterruptedException, ExecutionException {
		semaphore.acquireUninterruptibly();

		final Iterator<Future> iterator = futures.iterator();

		// Block while waiting on all of the futures to complete.
		while (iterator.hasNext()) {
			final Future future = iterator.next();

			future.get();
			iterator.remove();
		}

		semaphore.release();
	}

	private void lazilyExtract(Path file) {

		// Check status in reporter. Skip if good.
		if (null != reporter && reporter.succeeded(file)) {
			logger.info("File already extracted; skipping: " + file + ".");
			return;
		}

		final int status = extract(file);

		// Save status to registry and start a new job.
		if (null != reporter) {
			reporter.save(file, status);
		}
	}

	private int extract(Path file) {
		logger.info("Beginning extraction: " + file + ".");

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
		} catch (Throwable e) {
			logger.log(Level.SEVERE, "The extracted text could not be outputted: " + file + ".", e);
			status = Reporter.NOT_SAVED;
		}

		try {
			reader.close();
		} catch (IOException e) {
			logger.log(Level.SEVERE, "Error while closing file: " + file + ".", e);
		}

		if (Reporter.SUCCEEDED == status) {
			logger.info("Finished outputting file: " + file + ".");
		}

		return status;
	}
}
