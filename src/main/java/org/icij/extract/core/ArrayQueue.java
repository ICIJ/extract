package org.icij.extract.core;

import java.nio.file.Path;

import java.util.concurrent.ArrayBlockingQueue;

/**
 * A {@link Queue} using an array as a backend.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public class ArrayQueue extends ArrayBlockingQueue<Path> implements Queue {

	public static ArrayQueue create(final int capacity) {
		return new ArrayQueue(capacity);
	}

	public ArrayQueue(final int capacity) {
		super(capacity);
	}

	@Override
	public void close() {
		clear();
	}
}
