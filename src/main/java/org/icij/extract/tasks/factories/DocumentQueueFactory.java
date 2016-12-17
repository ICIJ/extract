package org.icij.extract.tasks.factories;

import org.icij.extract.document.Document;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.queue.DocumentQueue;
import org.icij.extract.DocumentQueueType;
import org.icij.extract.queue.ArrayDocumentQueue;
import org.icij.extract.queue.RedisDocumentQueue;
import org.icij.task.Options;

/**
 * Factory methods for creating queue objects.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public class DocumentQueueFactory {

	private DocumentQueueType type = null;
	private Options<String> options = null;
	private DocumentFactory factory = null;

	/**
	 * Prefers an in-local-memory queue by default.
	 *
	 * @param options options for creating the queue
	 */
	public DocumentQueueFactory(final Options<String> options) {
		type = options.get("queue-type").parse().asEnum(DocumentQueueType::parse).orElse(DocumentQueueType.ARRAY);
		this.options = options;
	}

	/**
	 * Set the factory used for creating {@link Document} objects from the queue.
	 *
	 * If none is set, a default instance will be created using the given options.
	 *
	 * @param factory the factory to use
	 * @return chainable factory
	 */
	public DocumentQueueFactory withDocumentFactory(final DocumentFactory factory) {
		this.factory = factory;
		return this;
	}

	/**
	 * Creates {@code Queue} based on the given arguments.
	 *
	 * @return a {@code Queue} or {@code null}
	 * @throws IllegalArgumentException if the arguments do not contain a valid queue type
	 */
	public DocumentQueue create() throws IllegalArgumentException {
		if (DocumentQueueType.ARRAY == type) {
			return new ArrayDocumentQueue(options.get("queue-buffer").parse().asInteger().orElse(1024));
		}

		return createShared();
	}

	/**
	 * Creates a share {@code Queue} based on the given commandline arguments, preferring Redis by default.
	 *
	 * @return a {@code Queue} or {@code null}
	 * @throws IllegalArgumentException if the given options do not contain a valid shared queue type
	 */
	public DocumentQueue createShared() throws IllegalArgumentException {
		if (null == factory) {
			factory = new DocumentFactory().configure(options);
		}

		if (DocumentQueueType.REDIS == type) {
			return new RedisDocumentQueue(factory, options);
		}

		throw new IllegalArgumentException(String.format("\"%s\" is not a valid shared queue type.", type));
	}
}
