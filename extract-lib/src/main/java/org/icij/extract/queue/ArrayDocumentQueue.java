package org.icij.extract.queue;

import org.icij.extract.document.TikaDocument;
import org.icij.task.Options;
import org.icij.task.annotation.Option;

import java.util.concurrent.ArrayBlockingQueue;

/**
 * A {@link DocumentQueue} using an array as a backend.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
@Option(name = "queueBuffer", description = "The size of the internal file path buffer used by the queue.",
		parameter = "size")
public class ArrayDocumentQueue extends ArrayBlockingQueue<TikaDocument> implements DocumentQueue {

	private static final long serialVersionUID = -7491630465350342533L;

	ArrayDocumentQueue(final Options<String> options) {
		this(options.get("queueBuffer").parse().asInteger().orElse(1024));
	}

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
