package org.icij.extract.queue;

import org.icij.extract.document.Document;
import org.icij.extract.document.DocumentFactory;

import org.icij.extract.mysql.DataSourceFactory;
import org.icij.task.Options;
import org.icij.task.annotation.Option;
import org.icij.task.annotation.OptionsClass;

/**
 * Factory methods for creating queue objects.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
@Option(name = "queueType", description = "Set the queue backend type. Valid values \"redis\" and \"mysql\".",
		parameter = "type",	code = "q")
@OptionsClass(DocumentFactory.class)
@OptionsClass(DataSourceFactory.class)
@OptionsClass(ArrayDocumentQueue.class)
@OptionsClass(RedisDocumentQueue.class)
@OptionsClass(MySQLDocumentQueue.class)
public class DocumentQueueFactory {

	private DocumentQueueType type = null;
	private Options<String> options = null;
	private DocumentFactory documentFactory = null;
	private DataSourceFactory dataSourceFactory = null;

	/**
	 * Prefers an in-local-memory queue by default.
	 *
	 * @param options options for creating the queue
	 */
	public DocumentQueueFactory(final Options<String> options) {
		type = options.get("queueType").parse().asEnum(DocumentQueueType::parse).orElse(DocumentQueueType.ARRAY);
		this.options = options;
	}

	/**
	 * Set the documentFactory used for creating {@link Document} objects from the queue.
	 *
	 * If none is set, a default instance will be created using the given options.
	 *
	 * @param factory the documentFactory to use
	 * @return chainable documentFactory
	 */
	public DocumentQueueFactory withDocumentFactory(final DocumentFactory factory) {
		this.documentFactory = factory;
		return this;
	}

	/**
	 * Set the data source factory for SQL-backed queues.
	 *
	 * If none is set, a default instance will be created using the given options.
	 *
	 * @param dataSourceFactory the data source factory to use
	 * @return chainable documentFactory
	 */
	public DocumentQueueFactory withDataSource(final DataSourceFactory dataSourceFactory) {
		this.dataSourceFactory = dataSourceFactory;
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
			return new ArrayDocumentQueue(options);
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
		if (null == documentFactory) {
			documentFactory = new DocumentFactory().configure(options);
		}

		if (DocumentQueueType.REDIS == type) {
			return new RedisDocumentQueue(documentFactory, options);
		}

		if (DocumentQueueType.MYSQL == type) {
			if (null == dataSourceFactory) {
				dataSourceFactory = new DataSourceFactory(options);
			}

			return new MySQLDocumentQueue(dataSourceFactory.get(), documentFactory, options);
		}

		throw new IllegalArgumentException(String.format("\"%s\" is not a valid shared queue type.", type));
	}
}
