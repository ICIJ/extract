package org.icij.extract.core;

import java.util.concurrent.BlockingQueue;

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
 * This implementation is thread-safe.
 *
 * @since 1.0.0-beta
 */
public class QueueingScanner extends Scanner {

	private final BlockingQueue<String> queue;

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

	@Override
	protected void handle(final Path file) throws InterruptedException {
		queue.put(file.toString());
	}
}
