package org.icij.extract.core;

import java.util.logging.Logger;

import java.util.concurrent.TimeUnit;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.nio.file.Path;

import java.util.concurrent.BlockingQueue;

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
	public static final long DEFAULT_TIMEOUT = 500L;
	public static final TimeUnit DEFAULT_TIMEOUT_UNIT = TimeUnit.MILLISECONDS;

	private final BlockingQueue<String> queue;

	private volatile boolean started = false;
	private long pollTimeout = DEFAULT_TIMEOUT;
	private TimeUnit pollTimeoutUnit = DEFAULT_TIMEOUT_UNIT;

	public PollingConsumer(Logger logger, BlockingQueue<String> queue, Spewer spewer, Extractor extractor, int threads) {
		super(logger, spewer, extractor, threads);
		this.queue = queue;
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

		if (1 == matcher.groupCount() || matcher.group(2).equals("ms")) {
			setPollTimeout(timeout, unit);
			return;
		}

		if (matcher.group(2).equals("h")) {
			unit = TimeUnit.HOURS;
		} else if (matcher.group(2).equals("m")) {
			unit = TimeUnit.MINUTES;
		} else if (matcher.group(2).equals("s")) {
			unit = TimeUnit.SECONDS;
		}

		setPollTimeout(timeout, unit);
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
		logger.info("Polling the queue, waiting up to " + pollTimeoutUnit.toMillis(pollTimeout) + "ms.");

		String file = null;

		try {
			file = queue.poll(pollTimeout, pollTimeoutUnit);
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
