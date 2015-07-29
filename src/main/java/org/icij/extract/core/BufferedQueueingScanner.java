package org.icij.extract.core;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import java.util.logging.Logger;

import java.nio.file.Path;

/**
 * An implementation of {@link Scanner} which pushes encountered file paths into a
 * given buffer, then draining that buffer to a given queue when the buffer nears
 * capacity. 
 *
 * Use this class when the underlying queue suffers from latency that would slow
 * down an otherwise fast directory scanning operation. Otherwise use the regular
 * directory scanner.
 *
 * Paths are pushed into the buffer synchronously and if it is bounded, only
 * when a space becomes available.
 *
 * The treshold at which draining occurs is defined as 60% of the buffer's
 * capacity. Draining is an asynchronous operation that runs on a background
 * thread.
 *
 * This implementation is thread-safe.
 *
 * @since 1.0.0-beta
 */
public class BufferedQueueingScanner extends Scanner {

	private final BlockingQueue<String> queue;
	private final BlockingQueue<String> buffer;
	private final ExecutorService drainer;
	private final AtomicBoolean draining;
	private final int threshold;

	/**
	 * Creates a {@code BufferedQueueingScanner} that buffers all results from the
	 * the scanner in a {@link BlockingQueue}, which should be bounded.
	 *
	 * When the internal queue is nearing capacity, a separate thread is
	 * started which drains it to the underlying queue.
	 *
	 * @param logger logger
	 * @param queue results from the scanner will be put on this queue
	 * @param buffer buffer
	 */
	public BufferedQueueingScanner(final Logger logger, final BlockingQueue<String> queue,
		final BlockingQueue<String> buffer) {
		super(logger);
		this.queue = queue;
		this.buffer = buffer;
		this.drainer = Executors.newSingleThreadExecutor();
		this.draining = new AtomicBoolean();
		this.threshold = (int) Math.ceil((double) buffer.remainingCapacity() / 1.6);
	}

	public BufferedQueueingScanner(final Logger logger, final BlockingQueue<String> queue,
		final int size) {
		this(logger, queue, new ArrayBlockingQueue<String>(size));
	}

	@Override
	protected void handle(final Path file) throws InterruptedException {
		buffer.put(file.toString());
		if (buffer.size() > threshold && draining.compareAndSet(false, true)) {
			drainer.execute(new DrainingTask());
		}
	}

	@Override
	public void awaitTermination() throws InterruptedException {
		super.awaitTermination();

		// When the scanner finishes, flush the buffer.
		if (buffer.size() > 0 && draining.compareAndSet(false, true)) {
			drainer.execute(new DrainingTask());
		}

		drainer.shutdown();
		logger.info("Awaiting completion of drainer.");
		while (!drainer.awaitTermination(60, TimeUnit.SECONDS)) {
			logger.info("Awaiting completion of drainer.");
		}
	}

	/**
	 * Runnable task that drains the internal buffer to an underlying queue.
	 *
	 * An implementation of {@link FutureTask} that returns the number of items
	 * drained as its result.
	 */
	protected class DrainingTask extends FutureTask<Integer> {

		protected DrainingTask() {
			super(new DrainingCall());
		}

		@Override
		protected void done() {
			draining.set(false);
		}
	}

	/*
	 * An implementation of {@link Callable} that returns the number of items
	 * drained as its result.
	 */
	protected class DrainingCall implements Callable<Integer> {

		@Override
		public Integer call() throws Exception {
			logger.info("Scanner draining to slow queue.");
			return new Integer(buffer.drainTo(queue));
		}
	}
}
