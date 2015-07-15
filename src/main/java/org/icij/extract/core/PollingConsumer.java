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
 * When polling is started using {@link start}, the consumer will automatically saturate
 * the thead pool with tasks. The consumer continues to poll in a loop so that the thread
 * pool remains saturated until the queue starts to drain off.
 *
 * @since 1.0.0-beta
 */
public class PollingConsumer extends Consumer {
	public static final TimeDuration DEFAULT_TIMEOUT = new TimeDuration(5L, TimeUnit.SECONDS);

	private final BlockingQueue<String> queue;
	private final AtomicBoolean started = new AtomicBoolean();

	private TimeDuration pollTimeout = DEFAULT_TIMEOUT;

	public PollingConsumer(Logger logger, BlockingQueue<String> queue, Spewer spewer, Extractor extractor, int parallelism) {
		super(logger, spewer, extractor, parallelism);
		this.queue = queue;
	}

	public void setPollTimeout(TimeDuration pollTimeout) {
		this.pollTimeout = pollTimeout;
	}

	public void setPollTimeout(String duration) throws IllegalArgumentException {
		setPollTimeout(new TimeDuration(duration));
	}

	/**
	 * Start consuming. This method blocks until the queue is drained.
	 *
	 * It's up to the user to stop the consumer if the thread is
	 * interrupted.
	 *
	 * @throws InterruptedException if interrupted while draining
	 */
	public void start() throws InterruptedException {
		logger.info("Starting consumer.");

		if (!started.compareAndSet(false, true)) {
			throw new IllegalStateException("Already started.");
		}

		String file;
		while (null != (file = poll()) && started.get()) {
			consume(file);
		}

		drained();
	}

	/**
	 * Stop consuming.
	 *
	 * @return Whether the thread was stopped or already stopped.
	 */
	public boolean stop() {
		return started.compareAndSet(false, true);
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
	 * @throws InterruptedException if interrupted while polling
	 */
	protected String poll() throws InterruptedException {
		logger.info(String.format("Polling the queue, waiting up to %s.", pollTimeout));

		String file = null;

		try {
			file = queue.poll(pollTimeout.getDuration(), pollTimeout.getUnit());
		} catch (NullPointerException e) {

			// This is a temporary bodge for:
			// https://github.com/mrniko/redisson/issues/181
			return null;
		}

		return file;
	}

	protected class ConsumerTask extends Consumer.ConsumerTask {

		public ConsumerTask(final Path file) {
			super(file);
		}

		protected int extract(final Path file) {
			final int status = extract(file);

			// If the file could not be written due to a storage endpoint error, put it back onto the queue.
			if (Reporter.NOT_SAVED == status) {
				queue.add(file.toString());
			}

			return status;
		}
	}
}
