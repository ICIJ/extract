package org.icij.extract.core;

import java.lang.Runtime;

import java.util.Iterator;
import java.util.HashSet;
import java.util.Set;

import java.util.logging.Level;
import java.util.logging.Logger;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

import java.nio.file.Path;
import java.nio.file.Paths;

import java.io.Reader;
import java.io.IOException;
import java.io.FileNotFoundException;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.exception.TikaException;
import org.apache.tika.exception.EncryptedDocumentException;

/**
 * Base consumer for file paths. Superclasses should call {@link #accept(Path)}.
 * All tasks are sent to a work-stealing thread pool.
 *
 * The parallelism of the thread pool is defined in the call to the constructor.
 *
 * A task is defined as both the extraction from a file and the ouputting of extracted data.
 * Completion is only considered successful if both parts of the task complete with no exceptions.
 *
 * The final status of each task is saved to the reporter, if any is set.
 *
 * @since 1.0.0-beta
 */
public class Consumer {
	public static final int DEFAULT_PARALLELISM = Runtime.getRuntime().availableProcessors();

	protected final Logger logger;
	protected final Spewer spewer;
	protected final Extractor extractor;
	protected final Semaphore permits;
	protected final ExecutorService executor;
	protected final int parallelism;

	protected Reporter reporter = null;

	/**
	 * Create a new consumer with the given parallelism. Calls to {@link #consume}
	 * will block when the given parallelism is reached.
	 *
	 * @param logger logger
	 * @param spewer spewer
	 * @param extractor extractor
	 * @param parallelism parallelism
	 */
	public Consumer(final Logger logger, final Spewer spewer,
		final Extractor extractor, final int parallelism) {
		this.logger = logger;
		this.spewer = spewer;
		this.extractor = extractor;
		this.parallelism = parallelism;
		this.executor = Executors.newFixedThreadPool(parallelism);
		this.permits = new Semaphore(parallelism);
	}

	/**
	 * Set the reporter.
	 *
	 * @param reporter reporter
	 */
	public void setReporter(final Reporter reporter) {
		this.reporter = reporter;
	}

	/**
	 * Consume a file. Like {@link #consume} but accepts a {@link String} path.
	 *
	 * @param file file path
	 * @throws InterruptedException if interrupted while waiting for a slot
	 */
	public void accept(final String file) throws InterruptedException {
		accept(Paths.get(file));
	}

	/**
	 * Consume a file.
	 *
	 * Jobs are put in an bounded queue and executed in serial, in a separate thread.
	 * This method blocks when the queue bound is reached to avoid flooding the consumer.
	 *
	 * @param file file path
	 * @throws InterruptedException if interrupted while waiting for a slot
	 */
	public void accept(final Path file) throws InterruptedException {
		logger.info(String.format("Sending to thread pool; will queue if full: %s.", file));
		permits.acquire();
		executor.execute(new ConsumerTask(file));
	}

	/**
	 * Shuts down the executor, then blocks until all the queued tasks have finished
	 * and the thread pool is empty.
	 *
	 * This method should be called to free up resources when the consumer
	 * is no longer needed.
	 *
	 * @throws InterruptedException if interrupted while waiting
	 */
	public void finish() throws InterruptedException {
		logger.info("Shutting down consumer executor.");
		executor.shutdown();

		do {
			logger.info("Awaiting completion of consumer.");
		} while (!executor.awaitTermination(60, TimeUnit.SECONDS));
		logger.info("Consumer finished.");
	}

	/**
	 * Send a file to the {@link Extractor} and return the result.
	 *
	 * @param file file path
	 * @return The result code.
	 */
	protected ReportResult extract(final Path file) {
		logger.info("Beginning extraction: " + file + ".");

		final Metadata metadata = new Metadata();

		Reader reader = null;
		ReportResult status = ReportResult.SUCCEEDED;

		try {
			reader = extractor.extract(file, metadata);
			logger.info(String.format("Outputting: %s.", file));
			spewer.write(file, metadata, reader);

		// SpewerException is thrown exclusively due to an output endpoint error.
		// It means that extraction succeeded, but the result could not be saved.
		} catch (SpewerException e) {
			logger.log(Level.SEVERE, String.format("The extraction result could not be outputted: %s.", file), e);
			status = ReportResult.NOT_SAVED;
		} catch (FileNotFoundException e) {
			logger.log(Level.SEVERE, String.format("File not found: %s. Skipping.", file), e);
			status = ReportResult.NOT_FOUND;
		} catch (IOException e) {

			// ParsingReader#read catches exceptions and wraps them in an IOException.
			final Throwable c = e.getCause();

			if (c instanceof ExcludedMediaTypeException) {
				logger.log(Level.INFO, String.format("The document was not parsed because all of the " +
					"parsers that handle it were excluded: %s.", file), e);
				status = ReportResult.NOT_PARSED;
			} else if (c instanceof EncryptedDocumentException) {
				logger.log(Level.SEVERE, String.format("Skipping encrypted file: %s.", file), e);
				status = ReportResult.NOT_DECRYPTED;

			// TIKA-198: IOExceptions thrown by parsers will be wrapped in a TikaException.
			// This helps us differentiate input stream exceptions from output stream exceptions.
			// https://issues.apache.org/jira/browse/TIKA-198
			} else if (c instanceof TikaException) {
				logger.log(Level.SEVERE, String.format("The document could not be parsed: %s.", file), e);
				status = ReportResult.NOT_PARSED;
			} else {
				logger.log(Level.SEVERE, String.format("The document stream could not be read: %s.", file), e);
				status = ReportResult.NOT_READ;
			}
		} catch (Throwable e) {
			logger.log(Level.SEVERE, String.format("Unknown exception during extraction or output: %s.", file), e);
			status = ReportResult.NOT_CLEAR;
		}

		try {
			reader.close();
		} catch (IOException e) {
			logger.log(Level.SEVERE, String.format("Error while closing extraction reader: %s.", file), e);
		}

		if (ReportResult.SUCCEEDED == status) {
			logger.info(String.format("Finished outputting file: %s.", file));
		}

		return status;
	}

	protected class ConsumerTask implements Runnable, Callable<Path> {

		protected final Path file;

		public ConsumerTask(final Path file) {
			this.file = file;
		}

		@Override
		public void run() {
			try {
				call();
			} catch (Exception e) {
				logger.log(Level.SEVERE, String.format("Exception while consuming file: %s.",
					file), e);
			} finally {
				permits.release();
			}
		}

		@Override
		public Path call() throws Exception {

			// Check status in reporter. Skip if good.
			if (null != reporter && reporter.check(file, ReportResult.SUCCEEDED)) {
				logger.info(String.format("File already extracted; skipping: %s.", file));

			// Save status to registry and start a new job.
			} else if (null != reporter) {
				reporter.save(file, extract(file));
			} else {
				extract(file);
			}

			return file;
		}
	}
}
