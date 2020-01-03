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
@Option(name = "queueName", description = "The name of the queue.", parameter = "name")
@Option(name = "queueBuffer", description = "The size of the internal file path buffer used by the queue.",
		parameter = "size")
public class MemoryDocumentQueue extends ArrayBlockingQueue<Path> implements DocumentQueue {

	private static final long serialVersionUID = -7491630465350342533L;
	private final String queueName;

	MemoryDocumentQueue(final Options<String> options) {
		this(options.get("queueName").value().orElse("extract:queue"), options.get("queueBuffer").parse().asInteger().orElse(1024));
	}

	/**
	 * Instantiate a new {@code ArrayPathQueue} with the given capacity.
	 *
	 * @param queueName name of the queue
	 * @param capacity the capacity of the queue
	 */
	public MemoryDocumentQueue(String queueName, final int capacity) {
		super(capacity);
		this.queueName = queueName;
	}

	@Override
	public void close() {
		clear();
	}

	@Override
	public String getName() {
		return queueName;
	}
}
