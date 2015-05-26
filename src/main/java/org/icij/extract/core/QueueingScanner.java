package org.icij.extract.core;

import java.util.Queue;

import java.util.logging.Logger;

import java.nio.file.Path;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public class QueueingScanner extends Scanner {

	private final Queue queue;

	public QueueingScanner(Logger logger, Queue queue) {
		super(logger);
		this.queue = queue;
	}

	protected void handle(Path file) {
		queue.add(file);
	}
}
