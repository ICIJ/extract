package org.icij.extract.core;

import org.icij.extract.interval.TimeDuration;

import java.util.logging.Logger;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.BlockingQueue;

import java.nio.file.Path;

/**
 * An implementation of {@link Consumer} which polls a given queue for paths to consume.
 * The queue should contain paths serialized to {@link String} objects.
 *
 * When polling is started using {@link start}, the consumer will automatically saturate
 * the thead pool with tasks. The consumer continues to poll in a loop so that the thread
 * pool remains saturated until the queue starts to drain off.
 *
 * @since 1.0.0-beta
 */
public class PollingConsumer extends Consumer {
	public static final TimeDuration DEFAULT_TIMEOUT = new TimeDuration(5L, TimeUnit.SECONDS);

	private final BlockingQueue<String> queue;

	private volatile boolean started = false;
	private TimeDuration pollTimeout = DEFAULT_TIMEOUT;

	public PollingConsumer(Logger logger, BlockingQueue<String> queue, Spewer spewer, Extractor extractor, int threads) {
		super(logger, spewer, extractor, threads);
		this.queue = queue;
	}

	public void setPollTimeout(TimeDuration pollTimeout) {
		this.pollTimeout = pollTimeout;
	}

	public void setPollTimeout(String duration) throws IllegalArgumentException {
		setPollTimeout(new TimeDuration(duration));
	}

	/**
	 * Start consuming.
	 */
	public void start() {
		logger.info("Starting consumer.");

		if (started) {
			throw new IllegalStateException("Already started.");
		}

		started = true;

		String file;

		while (started && null != (file = poll())) {
			consume(file);
		}

		drained();
	}

	/**
	 * Stop consuming.
	 */
	public void stop() {
		started = false;
	}

	protected void drained() {
		logger.info("Queue drained.");
	}

	protected String poll() {
		logger.info(String.format("Polling the queue, waiting up to %s.", pollTimeout));

		String file = null;

		try {
			file = queue.poll(pollTimeout.getDuration(), pollTimeout.getUnit());
		} catch (NullPointerException e) {

			// This is a temporary bodge for:
			// https://github.com/mrniko/redisson/issues/181
			return file;
		} catch (InterruptedException e) {
			logger.info("Thread interrupted while waiting to poll.");
			Thread.currentThread().interrupt();
		}

		return file;
	}

	protected class ConsumerTask extends Consumer.ConsumerTask {

		public ConsumerTask(final Path file) {
			super(file);
		}

		protected int extract(final Path file) {
			final int status = super.extract(file);

			// If the file could not be written due to a storage endpoint error, put it back onto the queue.
			if (Reporter.NOT_SAVED == status) {
				queue.add(file.toString());
			}

			return status;
		}
	}
}
