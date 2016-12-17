package org.icij.extract.queue;

import org.icij.extract.document.Document;

import java.util.concurrent.ArrayBlockingQueue;

/**
 * A {@link DocumentQueue} using an array as a backend.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public class ArrayDocumentQueue extends ArrayBlockingQueue<Document> implements DocumentQueue {

	private static final long serialVersionUID = -7491630465350342533L;

	/**
	 * Instantiate a new {@code ArrayPathQueue} with the given capacity.
	 *
	 * @param capacity the capacity of the queue
	 */
	public ArrayDocumentQueue(final int capacity) {
		super(capacity);
	}

	@Override
	public void close() {
		clear();
	}
}
