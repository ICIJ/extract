package org.icij.extract.core;

import java.lang.Runtime;

import java.util.Iterator;
import java.util.HashSet;
import java.util.Set;

import java.util.logging.Level;
import java.util.logging.Logger;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ExecutionException;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

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
	protected Reporter reporter = null;

	protected final ThreadPoolExecutor executor;
	protected int threads;

	private Charset outputEncoding = StandardCharsets.UTF_8;

	private final Semaphore pending;

	private final Extractor extractor;

	public Consumer(Logger logger, Spewer spewer, int threads) {
		this.logger = logger;
		this.spewer = spewer;

		this.threads = threads;
		this.pending = new Semaphore(threads);

		this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(threads);
		this.extractor = new Extractor(logger);
	}

	public void setOutputEncoding(Charset outputEncoding) {
		this.outputEncoding = outputEncoding;
	}

	public void setOutputEncoding(String outputEncoding) {
		setOutputEncoding(Charset.forName(outputEncoding));
	}

	public void setOcrLanguage(String ocrLanguage) {
		extractor.setOcrLanguage(ocrLanguage);
	}

	public void setOcrTimeout(int ocrTimeout) {
		extractor.setOcrTimeout(ocrTimeout);
	}

	public void setReporter(Reporter reporter) {
		this.reporter = reporter;
	}

	public void disableOcr() {
		extractor.disableOcr();
	}

	public void consume(final String file) {
		consume(Paths.get(file));
	}

	public void consume(final Path file) {
		logger.info("Sending to thread pool; will queue if full (" + executor.getActiveCount() + " active): " + file + ".");

		pending.acquireUninterruptibly();
		executor.submit(new Runnable() {

			@Override
			public void run() {
				lazilyExtract(file);
				pending.release();
			}
		});
	}

	public void start() {
		logger.info("Starting consumer.");
	}

	public void finish() throws InterruptedException, ExecutionException {
		logger.info("Consumer waiting for all threads to finish.");

		// Block until the thread pool is completely empty.
		pending.acquire(threads);

		logger.info("All threads finished. Shutting down executor.");
		shutdown();
	}

	public void shutdown() throws InterruptedException {
		logger.info("Shutting down executor.");

		executor.shutdown();

		// Set a very long timeout to allow for Tesseract processes to finish.
		// TODO: Make a call to the extractor to stop recursive extraction and throw an exception so that the file is returned to the queue.
		if (!executor.awaitTermination(1, TimeUnit.HOURS)) {
			logger.warning("Executor did not shut down in a timely manner.");
			executor.shutdownNow();
		} else {
			logger.info("Executor shut down successfully.");
		}
	}

	protected void lazilyExtract(Path file) {

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

	protected int extract(Path file) {
		logger.info("Beginning extraction: " + file + ".");

		ParsingReader reader = null;
		int status = Reporter.SUCCEEDED;

		try {
			reader = extractor.extract(file);
			logger.info("Outputting: " + file + ".");
			spewer.write(file, reader, outputEncoding);

		// SpewerException is thrown exclusively due to an output endpoint error.
		// It means that extraction succeeded, but the result could not be saved.
		} catch (SpewerException e) {
			logger.log(Level.SEVERE, "The extraction result could not be outputted: " + file + ".", e);
			status = Reporter.NOT_SAVED;
		} catch (FileNotFoundException e) {
			logger.log(Level.SEVERE, "File not found: " + file + ". Skipping.", e);
			status = Reporter.NOT_FOUND;
		} catch (IOException e) {
			logger.log(Level.SEVERE, "The document stream could not be read: " + file + ". Skipping.", e);
			status = Reporter.NOT_READ;
		} catch (EncryptedDocumentException e) {
			logger.log(Level.SEVERE, "Skipping encrypted file: " + file + ". Skipping.", e);
			status = Reporter.NOT_DECRYPTED;

		// TIKA-198: IOExceptions thrown by parsers will be wrapped in a TikaException.
		// This helps us differentiate input stream exceptions from output stream exceptions.
		// https://issues.apache.org/jira/browse/TIKA-198
		} catch (TikaException e) {
			logger.log(Level.SEVERE, "The document could not be parsed: " + file + ". Skipping.", e);
			status = Reporter.NOT_PARSED;
		} catch (Throwable e) {
			logger.log(Level.SEVERE, "Unknown exception during extraction or output: " + file + ". Skipping.", e);
			status = Reporter.NOT_CLEAR;
		}

		try {
			reader.close();
		} catch (IOException e) {
			logger.log(Level.SEVERE, "Error while closing extraction reader: " + file + ".", e);
		}

		if (Reporter.SUCCEEDED == status) {
			logger.info("Finished outputting file: " + file + ".");
		}

		return status;
	}
}
