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
 * @since 1.0.0-beta
 */
public class QueueingScanner extends Scanner {

	private final BlockingQueue<String> queue;

	public QueueingScanner(Logger logger, BlockingQueue<String> queue, Path path) {
		super(logger, path);
		this.queue = queue;
	}

	protected void handle(Path file) {
		try {
			queue.put(file.toString());
		} catch (InterruptedException e) {
			logger.warning("Interrupted while waiting for a free queue slot.");
			Thread.currentThread().interrupt();
		}
	}
}
