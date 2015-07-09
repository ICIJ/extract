package org.icij.extract.core;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

import java.util.logging.Logger;

import java.nio.file.Path;

/**
 * An implementation of {@link Scanner} which pushes encountered file paths into a
 * given queue. This is a classic producer, putting elements into a queue which
 * are then extracted by a consumer. 
 *
 * Paths are pushed into the queue synchronously and if the queue is bounded, only
 * when a space becomes available.
 *
 * A buffer size may be specified in the constructor, in which case a separate
 * thread will be used to drain to the queue when it is close to capacity.
 *
 * @since 1.0.0-beta
 */
public class QueueingScanner extends Scanner {

	private final BlockingQueue<String> queue;

	private BlockingQueue<String> slow = null;
	private int threshold = 0;
	private ExecutorService executor = null;

	/**
	 * Creates a {@code QueueingScanner} that sends all results from the
	 * {@link Scanner} straight to the underlying {@link BlockingQueue}.
	 *
	 * @param logger logger
	 * @param queue results from the scanner will be put on this queue
	 */
	public QueueingScanner(final Logger logger, final BlockingQueue<String> queue) {
		super(logger);
		this.queue = queue;
	}


	/**
	 * Creates a {@code QueueingScanner} that buffers all results from the
	 * the scanner in an {@link ArrayBlockingQueue} with the given capacity.
	 *
	 * When the internal queue is nearing capacity, a separate thread is
	 * started which drains it to the underlying queue.
	 *
	 * Use this constructor when the underlying queue suffers from latency
	 * that would slow down an otherwise fast directory scanning operation.
	 *
	 * @param logger logger
	 * @param queue results from the scanner will be put on this queue
	 */
	public QueueingScanner(final Logger logger, final BlockingQueue<String> queue,
		final int buffer) {
		this(logger, new ArrayBlockingQueue<String>(buffer));
		this.slow = queue;
		this.executor = Executors.newSingleThreadExecutor();
		this.threshold = (int) Math.ceil((double) buffer / 1.6);
	}

	@Override
	protected void handle(final Path file) {
		if (null != slow && queue.size() > threshold) {
			executor.submit(new DrainingTask());
		}

		try {
			queue.put(file.toString());
		} catch (InterruptedException e) {
			logger.warning("Interrupted while waiting for a free queue slot.");
			Thread.currentThread().interrupt();
		}
	}

	/**
	 * Runnable task that drains the internal queue to an underlying queue
	 * which is where we actually want the file paths to go.
	 */
	protected class DrainingTask implements Runnable {

		@Override
		public void run() {
			queue.drainTo(slow);
		}
	}
}
