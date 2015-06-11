package org.icij.extract.core;

import java.util.logging.Logger;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public class QueueingConsumer extends Consumer {

	public QueueingConsumer(Logger logger, Spewer spewer, Extractor extractor, int threads) {
		super(logger, spewer, extractor, threads);
	}
}
