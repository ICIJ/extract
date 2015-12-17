package org.icij.extract.core;

import org.icij.extract.interval.TimeDuration;

import java.util.logging.Logger;
import java.util.logging.Level;

import java.util.concurrent.Future;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import java.nio.file.Path;

/**
 * An implementation of {@link Consumer} which polls a given queue for paths to consume.
 * The queue should contain paths serialized to {@link String} objects.
 *
 * @since 1.0.0-beta
 */
public class PollingConsumer extends Consumer {
	public static final TimeDuration DEFAULT_TIMEOUT = new TimeDuration(0, TimeUnit.SECONDS);

	private final Queue queue;
	private final AtomicBoolean stopped = new AtomicBoolean();
	private final ExecutorService drainer = Executors.newCachedThreadPool();
	private final Semaphore draining = new Semaphore(1);

	private TimeDuration pollTimeout = DEFAULT_TIMEOUT;

	public PollingConsumer(final Logger logger, final Queue queue,
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
	 * Set the poll timeout in seconds.
	 *
	 * Setting to {@code 0} disables waiting.
	 */
	public void setPollTimeout(final long seconds) {
		setPollTimeout(new TimeDuration(seconds, TimeUnit.SECONDS));
	}

	/**
	 * Causes the consumer to wait until a new file is availabe,
	 * without any timeout.
	 *
	 * To not wait at all, call {@link setPollTimeout(long seconds)}.
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
	 * @return Whether draining completed successfully or was stopped.
	 * @throws InterruptedException if interrupted while draining
	 */
	public boolean drain() throws InterruptedException {
		logger.info("Draining consumer.");

		Path file;
		boolean stopped;

		draining.acquire();
		this.stopped.set(false);

		try {
			while (!(stopped = this.stopped.get()) && null != (file = poll())) {

				// If the consumer was stopped, put the file back
				// in queue and break.
				stopped = this.stopped.get();
				if (stopped) {
					queue.add(file);
					break;
				} else {
					accept(file);
				}
			}
		} finally {
			draining.release();
		}

		if (stopped) {
			logger.info("Draining stopped.");
			return false;
		} else {
			logger.info("Queue drained.");
			return true;
		}
	}

	/**
	 * Drain the queue in a non-blocking way, without ever timeing out,
	 * until the draining thread is interrupted or the task is cancelled.
	 *
	 * @return a {@link Future} represent the draining task
	 */
	public Future<?> drainContinuously() {
		return drainer.submit(new ContinuousDrain());
	}

	/**
	 * Stop draining.
	 *
	 * @return Whether draining was stopped or already stopped.
	 */
	public boolean stop() {
		return stopped.compareAndSet(false, true);
	}

	@Override
	public void finish() throws InterruptedException {
		draining.acquireUninterruptibly();
		drainer.shutdown();

		// Wait for the continuous drainer to send all pending tasks
		// the main executor service.
		try {
			while (!drainer.awaitTermination(60, TimeUnit.SECONDS));
			super.finish();
		} finally {
			draining.release();
		}
	}

	/**
	 * Poll the queue for a new file.
	 *
	 * Will wait for the duration set by {@link #setPollTimeout} or
	 * until a file becomes available if no timeout is set.
	 *
	 * @throws InterruptedException if interrupted while polling
	 */
	protected Path poll() throws InterruptedException {
		final Path file;

		if (null == pollTimeout) {
			logger.info("Polling the queue, waiting indefinitely.");
			file = queue.take();
		} else if (0 == pollTimeout.getDuration()) {
			logger.info("Polling the queue without waiting.");
			file = queue.poll();
		} else {
			logger.info(String.format("Polling the queue, waiting up to %s.", pollTimeout));
			file = queue.poll(pollTimeout.getDuration(), pollTimeout.getUnit());
		}

		return file;
	}

	/**
	 * A {@link Runnable} class that drains the queue until
	 * stopped or interrupted.
	 *
	 * Poison pills are difficult to use safely with a shared queue,
	 * so this consumer instead polls continuously at 1-second intervals,
	 * checking whether it should stop at each interval.
	 */
	private class ContinuousDrain implements Runnable {

		@Override
		public void run() {
			logger.info("Draining consumer until stopped or interrupted.");

			try {
				draining.acquire();
				stopped.set(false);
				while (!Thread.currentThread().isInterrupted() && !stopped.get()) {
					Path file = queue.poll(1L, TimeUnit.SECONDS);

					if (null != file) {
						accept(file);
					}
				}
			} catch (InterruptedException e) {
				logger.info("Continuous draining interrupted.");
				Thread.currentThread().interrupt();
			} finally {
				draining.release();
			}

			logger.info("Continuous draining stopped.");
		}
	}
}
