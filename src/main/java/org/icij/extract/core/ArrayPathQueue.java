package org.icij.extract.core;

import java.nio.file.Path;

import java.util.concurrent.ArrayBlockingQueue;

/**
 * A {@link PathQueue} using an array as a backend.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public class ArrayPathQueue extends ArrayBlockingQueue<Path> implements PathQueue {

	private static final long serialVersionUID = -7491630465350342533L;

	/**
	 * Create a new {@code ArrayPathQueue} with the given capacity.
	 *
	 * @param capacity the capacity of the queue
	 * @return a new queue
	 */
	public static ArrayPathQueue create(final int capacity) {
		return new ArrayPathQueue(capacity);
	}

	/**
	 * Instantiate a new {@code ArrayPathQueue} with the given capacity.
	 *
	 * @param capacity the capacity of the queue
	 */
	private ArrayPathQueue(final int capacity) {
		super(capacity);
	}

	@Override
	public void close() {
		clear();
	}
}
