package org.icij.extract.core;

import java.util.logging.Logger;

import java.nio.file.Path;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public class ConsumingScanner extends Scanner {

	private final QueueingConsumer consumer;

	public ConsumingScanner(Logger logger, QueueingConsumer consumer) {
		super(logger);
		this.consumer = consumer;
	}

	protected void handle(Path file) {
		consumer.consume(file);
	}
}
