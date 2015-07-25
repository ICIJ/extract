package org.icij.extract.core;

import org.icij.extract.interval.TimeDuration;

import java.util.logging.Logger;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import java.nio.file.Path;

/**
 * An implementation of {@link Consumer} which polls a given queue for paths to consume.
 * The queue should contain paths serialized to {@link String} objects.
 *
 * @since 1.0.0-beta
 */
public class PollingConsumer extends Consumer {
	public static final TimeDuration DEFAULT_TIMEOUT = new TimeDuration(5L, TimeUnit.SECONDS);

	private final BlockingQueue<String> queue;
	private final AtomicBoolean stopped = new AtomicBoolean();

	private TimeDuration pollTimeout = DEFAULT_TIMEOUT;

	public PollingConsumer(final Logger logger, final BlockingQueue<String> queue,
		final Spewer spewer, final Extractor extractor, final int parallelism) {
		super(logger, spewer, extractor, parallelism);
		this.queue = queue;
	}

	/**
	 * Set the amount of time to wait until an item becomes available.
	 *
	 * @param pollTimeout the amount of time to wait
	 */
	public void setPollTimeout(final TimeDuration pollTimeout) {
		this.pollTimeout = pollTimeout;
	}

	/**
	 * Set the amount of time to wait until an item becomes available.
	 *
	 * Accepts a duration string. For example {@code "2m"} for two minutes.
	 *
	 * @param duration the amount of time to wait
	 */
	public void setPollTimeout(final String duration) throws IllegalArgumentException {
		setPollTimeout(new TimeDuration(duration));
	}

	/**
	 * Causes the consumer to wait until a new file is availabe,
	 * without any timeout.
	 */
	public void clearPollTimeout() {
		pollTimeout = null;
	}

	/**
	 * Get the poll timeout.
	 *
	 * @return The poll timeout.
	 */
	public TimeDuration getPollTimeout() {
		return pollTimeout;
	}

	/**
	 * Consume and block until the queue is drained.
	 *
	 * It's up to the user to stop the consumer if the thread is
	 * interrupted.
	 *
	 * @throws InterruptedException if interrupted while draining
	 */
	public void drain() throws InterruptedException {
		logger.info("Draining consumer.");

		String file;
		while (null != (file = poll())) {
			if (!stopped.get()) {
				consume(file);

			// If the consumer was stopped, add the file back to
			// the queue and break.
			} else {
				queue.add(file);
				break;
			}
		}

		drained();
	}

	/**
	 * Stop consuming.
	 *
	 * @return Whether the consumer was stopped or already stopped.
	 */
	public boolean stop() {
		return stopped.compareAndSet(false, true);
	}

	/**
	 * Observer method that's called when the queue is drained.
	 */
	protected void drained() {
		logger.info("Queue drained.");
	}

	/**
	 * Poll the queue for a new file.
	 *
	 * Will wait for the duration set by {@link #setPollTimeout} or
	 * until a file becomes available if no timeout is set.
	 *
	 * @throws InterruptedException if interrupted while polling
	 */
	protected String poll() throws InterruptedException {
		if (null == pollTimeout) {
			logger.info("Polling the queue, waiting indefinitely.");
			return queue.take();
		}

		if (0 == pollTimeout.getDuration()) {
			logger.info("Polling the queue without waiting.");
			return queue.poll();
		}

		logger.info(String.format("Polling the queue, waiting up to %s.", pollTimeout));
		return queue.poll(pollTimeout.getDuration(), pollTimeout.getUnit());
	}

	/**
	 * Send a file to the {@link Extractor} and report the result and
	 * add the file back to the queue if extraction failed due to an
	 * I/O error.
	 *
	 * This helps maintain robust, failure resistant processing in
	 * cases of poor network connectivity.
	 *
	 * @param file file path
	 */
	@Override
	protected int extract(final Path file) {
		final int status = super.extract(file);

		// If the file could not be written due to a storage endpoint error,
		// or could not be read due to an I/O error other than the file not
		// being found, put it back onto the queue.
		if (Reporter.NOT_SAVED == status ||
			Reporter.NOT_READ == status) {
			logger.warning(String.format("Adding back to queue: %s.", file));
			queue.add(file.toString());
		}

		return status;
	}
}
