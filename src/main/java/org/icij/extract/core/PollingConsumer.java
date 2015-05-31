package org.icij.extract.core;

import java.util.logging.Logger;

import java.util.concurrent.TimeUnit;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.nio.file.Path;

import java.util.Queue;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public class PollingConsumer extends Consumer {
	public static final long DEFAULT_TIMEOUT = 500L;
	public static final TimeUnit DEFAULT_TIMEOUT_UNIT = TimeUnit.MILLISECONDS;

	private final Queue queue;

	private long pollTimeout = DEFAULT_TIMEOUT;
	private TimeUnit pollTimeoutUnit = DEFAULT_TIMEOUT_UNIT;

	public PollingConsumer(Logger logger, Queue queue, Spewer spewer, int threads) {
		super(logger, spewer, threads);
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

	public void consume() {
		logger.info("Polling the queue.");

		String file = (String) queue.poll();

		if (null == file && pollTimeout > 0L) {
			file = pollWait();
		}

		// Shut down the executor if the queue is empty.
		if (null == file) {
			drained();
		} else {
			consume(file);
			consume();
		}
	}

	public void start() {
		super.start();
		saturate();
	}

	public void saturate() {
		final int activeThreads = executor.getActiveCount();
		final int absentThreads = threads - activeThreads;

		logger.info("Saturating with " + absentThreads + " threads.");

		for (int i = 0; i < absentThreads; i++) {
			consume();
		}
	}

	protected void drained() {
		logger.info("Queue drained.");
	}

	private String pollWait() {
		logger.info("Polling the queue, waiting up to " + pollTimeout + " " + pollTimeoutUnit + ".");

		// TODO: Use wait/notify instead.
		try {
			Thread.sleep(TimeUnit.MILLISECONDS.convert(pollTimeout, pollTimeoutUnit));
		} catch (InterruptedException e) {
			logger.info("Thread interrupted while waiting to poll.");
			return null;
		}

		return (String) queue.poll();
	}
}
