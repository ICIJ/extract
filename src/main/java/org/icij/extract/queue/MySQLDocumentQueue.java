package org.icij.extract.queue;

import org.icij.extract.document.Document;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.mysql.DataSourceFactory;
import org.icij.sql.concurrent.MySQLBlockingQueueAdapter;
import org.icij.sql.concurrent.MySQLLock;
import org.icij.sql.concurrent.SQLBlockingQueue;
import org.icij.sql.concurrent.SQLQueueCodec;
import org.icij.task.Options;
import org.icij.task.annotation.Option;
import org.icij.task.annotation.OptionsClass;

import javax.sql.DataSource;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

@Option(name = "queueTable", description = "The queue table Defaults to \"document_queue\".", parameter = "name")
@Option(name = "queueIdKey", description = "For queues that provide an ID, use this option to specify the key.",
		parameter = "name")
@Option(name = "queuePathKey", description = "The table key for storing the document path. Must be unique in the " +
		"table. Defaults to \"path\".", parameter = "name")
@Option(name = "queueStatusKey", description = "The table key for storing the queue status.", parameter = "name")
@Option(name = "queueWaitingStatus", description = "The status value for waiting documents.", parameter = "value")
@Option(name = "queueProcessedStatus", description = "The status value for non-waiting documents.", parameter = "value")
@OptionsClass(DataSourceFactory.class)
public class MySQLDocumentQueue extends SQLBlockingQueue<Document> implements DocumentQueue {

	private static class DocumentQueueCodec implements SQLQueueCodec<Document> {

		private final DocumentFactory factory;
		private final String idKey;
		private final String pathKey;
		private final String statusKey;
		private final String waitingStatus;
		private final String processedStatus;

		DocumentQueueCodec(final DocumentFactory factory, final Options<String> options) {
			this.factory = factory;
			this.idKey = options.get("queueIdKey").value().orElse(null);
			this.pathKey = options.get("queuePathKey").value().orElse("path");
			this.statusKey = options.get("queueStatusKey").value().orElse("queue_status");
			this.waitingStatus = options.get("queueWaitingStatus").value().orElse("waiting");
			this.processedStatus = options.get("queueProcessedStatus").value().orElse("processed");
		}

		@Override
		public String getKeyName() {
			if (null != idKey) {
				return idKey;
			}

			return pathKey;
		}

		@Override
		public String encodeKey(final Object o) {
			if (null != idKey) {
				return ((Document) o).getId();
			}

			return ((Document) o).getPath().toString();
		}

		@Override
		public String getStatusKey() {
			return statusKey;
		}

		@Override
		public String getWaitingStatus() {
			return waitingStatus;
		}

		@Override
		public String getProcessedStatus() {
			return processedStatus;
		}

		@Override
		public Document decodeValue(final ResultSet rs) throws SQLException {
			final Path path = Paths.get(rs.getString(pathKey));

			if (null != idKey) {
				return factory.create(rs.getString(idKey), path);
			}

			return factory.create(path);
		}

		@Override
		public Map<String, Object> encodeValue(final Object o) {
			if (!(o instanceof Document)) {
				throw new IllegalArgumentException();
			}

			final Map<String, Object> map = new HashMap<>();

			if (null != idKey) {
				map.put(idKey, ((Document) o).getId());
			}

			map.put(pathKey, ((Document) o).getPath().toString());
			map.put(statusKey, waitingStatus);

			return map;
		}
	}

	MySQLDocumentQueue(final DocumentFactory factory, final Options<String> options) {

		// The queue should never need more than two connections per process: one to add and one to poll.
		this(new DataSourceFactory(options).withMaximumPoolSize(2).create("queuePool"),
				new DocumentQueueCodec(factory, options), options.get("queueTable").value().orElse("documents"));
	}

	private MySQLDocumentQueue(final DataSource ds, final SQLQueueCodec<Document> codec, final String table) {
		super(ds, new MySQLLock(ds, table), new MySQLBlockingQueueAdapter<>(codec, table));
	}

	@Override
	public void close() throws IOException {
		if (ds instanceof Closeable) {
			((Closeable) ds).close();
		}
	}
}
