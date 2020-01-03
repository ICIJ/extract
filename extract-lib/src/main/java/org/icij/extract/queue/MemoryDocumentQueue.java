package org.icij.extract.queue;

import org.icij.task.Options;
import org.icij.task.annotation.Option;

import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * A {@link DocumentQueue} using an array as a backend.
 *
 *
 */
@Option(name = "queueBuffer", description = "The size of the internal file path buffer used by the queue.",
		parameter = "size")
public class MemoryDocumentQueue extends ArrayBlockingQueue<Path> implements DocumentQueue {

	private static final long serialVersionUID = -7491630465350342533L;

	MemoryDocumentQueue(final Options<String> options) {
		this(options.get("queueBuffer").parse().asInteger().orElse(1024));
	}

	/**
	 * Instantiate a new {@code ArrayPathQueue} with the given capacity.
	 *
	 * @param capacity the capacity of the queue
	 */
	public MemoryDocumentQueue(final int capacity) {
		super(capacity);
	}

	@Override
	public void close() {
		clear();
	}
}
