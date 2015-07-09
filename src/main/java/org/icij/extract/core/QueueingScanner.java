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
	 * For each directory scanned, a separate thread is created which monitors
	 * the buffer capacity and drains it to the underlying queue.
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
	}

	@Override
	protected void handle(final Path file) {
		try {
			queue.put(file.toString());
		} catch (InterruptedException e) {
			logger.warning("Interrupted while waiting for a free queue slot.");
			Thread.currentThread().interrupt();
		}
	}

	@Override
	protected void scanDirectory(final Path directory) {
		if (null != slow) {
			final DrainingTask task = new DrainingTask();
			final ExecutorService executor = Executors.newSingleThreadExecutor();

			executor.submit(task);
			super.scanDirectory(directory);
			task.stop();
		} else {
			super.scanDirectory(directory);
		}
	}

	protected class DrainingTask implements Runnable {

		private volatile boolean stopped = false;

		public void stop() {
			stopped = true;
		}

		@Override
		public void run() {
			stopped = false;

			while (!stopped) {
				if (queue.size() > 1) {
					queue.drainTo(slow);
				}
			}
		}
	}
}
