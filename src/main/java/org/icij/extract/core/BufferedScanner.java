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
 * given or managed buffer, while a separate thread drains that buffer to the
 * slow buffer.
 *
 * Use this class when the underlying queue suffers from latency that would slow
 * down an otherwise fast directory scanning operation. Otherwise use the regular
 * directory scanner.
 *
 * Paths are pushed into the buffer synchronously and if it is bounded, only
 * when a space becomes available.
 *
 * This implementation is thread-safe.
 *
 * @since 1.0.0-beta
 */
public class BufferedScanner extends Scanner {

	/**
	 * The "poison pill" which is signify to the worker that it should stop.
	 */
	protected static final String POISON_PILL = "\0";

	protected final BlockingQueue<String> buffer;
	protected final ExecutorService drainer = Executors.newSingleThreadExecutor();

	/**
	 * Creates a {@code BufferedScanner} that buffers all results from the
	 * the scanner in a {@link BlockingQueue}, which should be bounded.
	 *
	 * On instantiation, a separate thread is started which drains it to the
	 * underlying queue.
	 *
	 * @param logger logger
	 * @param queue results from the scanner will be put on this queue
	 * @param buffer a fast queue buffer that will drain to the slow queue
	 */
	public BufferedScanner(final Logger logger, final BlockingQueue<String> queue,
		final BlockingQueue<String> buffer) {
		super(logger, queue);
		this.buffer = buffer;
		drainer.execute(new DrainingWorker());
	}

	/**
	 * Creates a {@code BufferedScanner} with a buffer of the given size.
	 *
	 * @param logger logger
	 * @param queue results from the scanner will be put on this queue
	 * @param size the desired buffer capacity
	 */
	public BufferedScanner(final Logger logger, final BlockingQueue<String> queue,
		final int size) {
		this(logger, queue, new ArrayBlockingQueue<String>(size));
	}

	@Override
	protected void queue(final Path file) throws InterruptedException {
		buffer.put(file.toString());
	}

	@Override
	public void finish() throws InterruptedException {
		super.finish();

		// When the scanner finishes, send a poison pill to the drainer
		// and shut it down.
		drainer.shutdown();
		buffer.put(POISON_PILL);
		do {
			logger.info("Awaiting completion of drainer.");
		} while (!drainer.awaitTermination(60, TimeUnit.SECONDS));
	}

	/**
	 * Runnable task that drains the internal buffer to an underlying queue.
	 */
	protected class DrainingWorker implements Runnable {

		/**
		 * Drains the queue in a loop until the current thread is interrupted
		 * or the buffer is poisoned.
		 */
		private void loop() throws Exception {
			while (!Thread.currentThread().isInterrupted()) {
				String file = buffer.take();

				if (!POISON_PILL.equals(file)) {
					queue.put(file);
				} else {
					break;
				}
			}
		}

		@Override
		public void run() {
			try {
				loop();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			} catch (Exception e) {
				logger.log(Level.SEVERE, "Exception while draining to queue.", e);
			}
		}
	}
}
