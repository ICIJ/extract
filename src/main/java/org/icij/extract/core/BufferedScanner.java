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
import java.util.logging.Level;

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
public class BufferedScanner extends Scanner {

	private final BlockingQueue<String> buffer;
	private final ExecutorService drainer = Executors.newSingleThreadExecutor();
	private final AtomicBoolean draining = new AtomicBoolean();
	private final int threshold;

	/**
	 * Creates a {@code BufferedScanner} that buffers all results from the
	 * the scanner in a {@link BlockingQueue}, which should be bounded.
	 *
	 * When the internal queue is nearing capacity, a separate thread is
	 * started which drains it to the underlying queue.
	 *
	 * @param logger logger
	 * @param queue results from the scanner will be put on this queue
	 * @param buffer a fast queue buffer that will drain to the slow queue
	 */
	public BufferedScanner(final Logger logger, final BlockingQueue<String> queue,
		final BlockingQueue<String> buffer) {
		super(logger, queue);
		this.buffer = buffer;
		this.threshold = (int) Math.ceil((double) buffer.remainingCapacity() / 1.6);
	}

	public BufferedScanner(final Logger logger, final BlockingQueue<String> queue,
		final int size) {
		this(logger, queue, new ArrayBlockingQueue<String>(size));
	}

	@Override
	protected void accept(final Path file) throws InterruptedException {
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
		do {
			logger.info("Awaiting completion of drainer.");
		} while (!drainer.awaitTermination(60, TimeUnit.SECONDS));
	}

	/**
	 * Runnable task that drains the internal buffer to an underlying queue.
	 *
	 * An implementation of {@link Callable} that returns the number of items
	 * drained as its result.
	 */
	protected class DrainingTask implements Runnable, Callable<Integer> {

		@Override
		public void run() {
			try {
				call();
			} catch (Exception e) {
				logger.log(Level.SEVERE, "Exception while draining to slow queue.", e);
			}
		}

		@Override
		public Integer call() throws Exception {
			logger.info("Scanner draining to slow queue.");
			return new Integer(buffer.drainTo(queue));
		}
	}
}
