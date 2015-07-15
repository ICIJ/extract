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
import java.util.concurrent.ExecutorService;

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

	protected final ExecutorService executor;
	protected int parallelism;

	protected Reporter reporter = null;
	protected Charset outputEncoding = StandardCharsets.UTF_8;

	private final Semaphore slots;

	public Consumer(Logger logger, Spewer spewer, Extractor extractor, int parallelism) {
		this.logger = logger;
		this.spewer = spewer;
		this.extractor = extractor;

		this.parallelism = parallelism;
		this.slots = new Semaphore(parallelism);

		this.executor = Executors.newWorkStealingPool(parallelism);
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
	 * @throws InterruptedException if interrupted while waiting for a slot
	 */
	public void consume(final String file) throws InterruptedException {
		consume(Paths.get(file));
	}

	/**
	 * Consume a file. If the thread pool is full, blocks until a slot is available.
	 * This behaviour allows polling consumers to poll in a loop without flooding
	 * the queue.
	 *
	 * @param file file path
	 * @throws InterruptedException if interrupted while waiting for a slot
	 */
	public void consume(final Path file) throws InterruptedException {
		logger.info(String.format("Sending to thread pool; will queue if full: %s.", file));

		slots.acquire();
		executor.execute(new ConsumerTask(file));
	}

	/**
	 * Blocks until all the consumer tasks have finished and the thread pool is empty.
	 *
	 * @throws InterruptedException if interrupted while waiting
	 */
	public void awaitTermination() throws InterruptedException {
		logger.info("Consumer waiting for all threads to finish.");

		// Block until the thread pool is completely empty.
		slots.acquire(parallelism);
		logger.info("All threads finished.");
	}

	/**
	 * Shut down the executor.
	 *
	 * This method should be called to free up resources when the consumer
	 * is no longer needed.
	 */
	public void shutdown() {
		logger.info("Shutting down consumer executor.");
		executor.shutdown();
	}

	/**
	 * Shut down the executor immediately.
	 *
	 * This method should be called to interrupt the consumer.
	 */
	public void shutdownNow() {
		logger.info("Forcibly shutting down scanner executor.");
		executor.shutdownNow();
	}

	protected int extract(final Path file) {
		logger.info("Beginning extraction: " + file + ".");

		final Metadata metadata = new Metadata();

		Reader reader = null;
		int status = Reporter.SUCCEEDED;

		try {
			reader = extractor.extract(file, metadata);
			logger.info("Outputting: " + file + ".");
			spewer.write(file, metadata, reader, outputEncoding);

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
	}
}
