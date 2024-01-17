package org.icij.extract.queue;

import org.apache.tika.metadata.Metadata;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.TikaDocument;
import org.icij.extract.mysql.SQLQueueCodec;
import org.icij.task.Options;
import org.icij.task.annotation.Option;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

@Option(name = "queueIdKey", description = "For queues that provide an ID, use this option to specify the key.",
		parameter = "name")
@Option(name = "queueForeignIdKey", description = "For queues that have a foreign ID column, specify the column name.",
		parameter = "name")
@Option(name = "queuePathKey", description = "The table key for storing the document path. Must be unique in the " +
		"table. Defaults to \"path\".", parameter = "name")
@Option(name = "queueSizeKey", description = "The table key for storing the document size, in bytes.", parameter =
		"name")
@Option(name = "queueStatusKey", description = "The table key for storing the queue status.", parameter = "name")
@Option(name = "queueWaitingStatus", description = "The status value for waiting documents.", parameter = "value")
@Option(name = "queueProcessedStatus", description = "The status value for non-waiting documents.", parameter = "value")
public class SQLDocumentQueueCodec<T> implements SQLQueueCodec<T> {

	private final DocumentFactory factory;
	private final String idKey;
	private final String foreignIdKey;
	private final String pathKey;
	private final String sizeKey;
	private final String statusKey;
	private final String waitingStatus;
	private final String processedStatus;
	private final Class<T> clazz;

	SQLDocumentQueueCodec(final DocumentFactory factory, final Options<String> options, Class<T> clazz) {
		this.factory = factory;
		this.idKey = options.get("queueIdKey").value().orElse(null);
		this.foreignIdKey = options.get("queueForeignIdKey").value().orElse(null);
		this.pathKey = options.get("queuePathKey").value().orElse("path");
		this.sizeKey = options.get("queueSizeKey").value().orElse("size");
		this.statusKey = options.get("queueStatusKey").value().orElse("queue_status");
		this.waitingStatus = options.get("queueWaitingStatus").value().orElse("waiting");
		this.processedStatus = options.get("queueProcessedStatus").value().orElse("processed");
		this.clazz = clazz;
	}

	@Override
	public Map<String, Object> encodeKey(final Object o) {
		final TikaDocument tikaDocument = (TikaDocument) o;
		final Map<String, Object> map = new HashMap<>();

		if (null != idKey) {
			map.put(idKey, tikaDocument.getId());
		} else {
			map.put(pathKey, tikaDocument.getPath().toString());
		}

		if (null != foreignIdKey) {
			map.put(foreignIdKey, tikaDocument.getForeignId());
		}

		return map;
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
	public T decodeValue(final ResultSet rs) throws SQLException {
		final Path path = Paths.get(rs.getString(pathKey));
		final long size = rs.getLong(sizeKey);
		final TikaDocument tikaDocument;

		if (null != idKey) {
			tikaDocument = factory.create(rs.getString(idKey), path, size);
		} else {
			tikaDocument = factory.create(path, size);
		}

		if (null != foreignIdKey) {
			tikaDocument.setForeignId(rs.getString(foreignIdKey));
		}

		return clazz.isAssignableFrom(Path.class) ?
				(T) tikaDocument.getPath() :
				(T) tikaDocument.getId();
	}

	@Override
	public Map<String, Object> encodeValue(final Object o) {
		final TikaDocument tikaDocument = (TikaDocument) o;
		final Map<String, Object> map = new HashMap<>();

		if (null != idKey) {
			map.put(idKey, tikaDocument.getId());
		}

		if (null != foreignIdKey) {
			map.put(foreignIdKey, tikaDocument.getForeignId());
		}

		map.put(pathKey, tikaDocument.getPath().toString());
		map.put(statusKey, waitingStatus);

		final String size = tikaDocument.getMetadata().get(Metadata.CONTENT_LENGTH);

		if (null != size) {
			map.put(sizeKey, Long.valueOf(size));
		}

		return map;
	}
}
