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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import java.io.Reader;
import java.io.IOException;
import java.io.FileNotFoundException;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.exception.TikaException;
import org.apache.tika.exception.EncryptedDocumentException;

/**
 * Base consumer for file paths. Superclasses should call {@link #consume(Path)}.
 * All tasks are sent to a fixed {@link ThreadPoolExecutor} which is backed by a queue.
 * The size of the thread pool is defined in the call to the constructor.
 *
 * A task is defined as both the extraction from a file and the ouputting of extracted data.
 * Completion is only considered successful if both parts of the task complete with no exceptions.
 *
 * The final status of each task is saved to the reporter, if any is set.
 *
 * @since 1.0.0-beta
 */
public class Consumer {
	public static final int DEFAULT_THREADS = Runtime.getRuntime().availableProcessors();

	protected final Logger logger;
	protected final Spewer spewer;
	protected final Extractor extractor;

	protected final ThreadPoolExecutor executor;
	protected int threads;

	protected Reporter reporter = null;
	protected Charset outputEncoding = StandardCharsets.UTF_8;

	private final Semaphore slots;

	public Consumer(Logger logger, Spewer spewer, Extractor extractor, int threads) {
		this.logger = logger;
		this.spewer = spewer;
		this.extractor = extractor;

		this.threads = threads;
		this.slots = new Semaphore(threads);

		this.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(threads);
	}

	public void setOutputEncoding(Charset outputEncoding) {
		this.outputEncoding = outputEncoding;
	}

	public void setOutputEncoding(String outputEncoding) {
		setOutputEncoding(Charset.forName(outputEncoding));
	}

	public void setReporter(Reporter reporter) {
		this.reporter = reporter;
	}

	/**
	 * Consume a file. If the thread pool is full, blocks until a slot is available.
	 * This behaviour allows polling consumers to poll in a loop.
	 *
	 * @param file file path
	 */
	public void consume(final String file) {
		consume(Paths.get(file));
	}

	/**
	 * Consume a file. If the thread pool is full, blocks until a slot is available.
	 * This behaviour allows polling consumers to poll in a loop.
	 *
	 * @param file file path
	 */
	public void consume(final Path file) {
		logger.info("Sending to thread pool; will queue if full (" + executor.getActiveCount() + " active): " + file + ".");

		slots.acquireUninterruptibly();
		executor.submit(new ConsumerTask(file));
	}

	/**
	 * Blocks until all the consumer tasks have finished and the thread pool is empty.
	 */
	public void awaitTermination() throws InterruptedException {
		logger.info("Consumer waiting for all threads to finish.");

		// Block until the thread pool is completely empty.
		slots.acquire(threads);
		logger.info("All threads finished.");
	}

	/**
	 * Shuts down the consumer and causes to longer accept new tasks.
	 * This method will forcibly shut down the consumer without waiting for pending tasks to finish.
	 * Call {@link #awaitTermination} first for a clean shut down.
	 */
	public void shutdown() throws InterruptedException {
		logger.info("Shutting down executor.");

		executor.shutdown();

		if (!executor.awaitTermination(1, TimeUnit.MINUTES)) {
			logger.warning("Executor did not shut down in a timely manner.");
			executor.shutdownNow();
		} else {
			logger.info("Executor shut down successfully.");
		}
	}

	protected class ConsumerTask implements Runnable {

		protected final Path file;

		public ConsumerTask(final Path file) {
			this.file = file;
		}

		@Override
		public void run() {

			// Check status in reporter. Skip if good.
			if (null != reporter && reporter.succeeded(file)) {
				logger.info("File already extracted; skipping: " + file + ".");

			// Save status to registry and start a new job.
			} else if (null != reporter) {
				reporter.save(file, extract(file));
			} else {
				extract(file);
			}

			slots.release();
		}

		protected int extract(final Path file) {
			logger.info("Beginning extraction: " + file + ".");

			final Metadata metadata = new Metadata();

			Reader reader = null;
			int status = Reporter.SUCCEEDED;

			try {
				reader = extractor.extract(file, metadata);
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

				// ParsingReader#read catches exceptions and wraps them in an IOException.
				final Throwable c = e.getCause();

				if (c instanceof ExcludedMediaTypeException) {
					logger.log(Level.INFO, "The document was not parsed because all of the parsers that handle it were excluded: " + file + ". Skipping.", e);
					status = Reporter.NOT_PARSED;
				} else if (c instanceof EncryptedDocumentException) {
					logger.log(Level.SEVERE, "Skipping encrypted file: " + file + ". Skipping.", e);
					status = Reporter.NOT_DECRYPTED;

				// TIKA-198: IOExceptions thrown by parsers will be wrapped in a TikaException.
				// This helps us differentiate input stream exceptions from output stream exceptions.
				// https://issues.apache.org/jira/browse/TIKA-198
				} else if (c instanceof TikaException) {
					logger.log(Level.SEVERE, "The document could not be parsed: " + file + ". Skipping.", e);
					status = Reporter.NOT_PARSED;
				} else {
					logger.log(Level.SEVERE, "The document stream could not be read: " + file + ". Skipping.", e);
					status = Reporter.NOT_READ;
				}
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
}
