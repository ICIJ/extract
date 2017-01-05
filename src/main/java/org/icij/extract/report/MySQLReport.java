package org.icij.extract.report;

import org.icij.extract.document.Document;
import org.icij.extract.extractor.ExtractionStatus;
import org.icij.extract.mysql.DataSourceFactory;
import org.icij.sql.concurrent.MySQLConcurrentMapAdapter;
import org.icij.sql.concurrent.SQLCodec;
import org.icij.sql.concurrent.SQLConcurrentMap;
import org.icij.task.Options;
import org.icij.task.annotation.Option;
import org.icij.task.annotation.OptionsClass;

import javax.sql.DataSource;
import java.io.Closeable;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

@Option(name = "reportTable", description = "The report table. Defaults to \"document_report\".", parameter = "name")
@Option(name = "reportIdKey", description = "For reports tables that have an ID column, specify the column name. This" +
		" will then be used as the unique key.", parameter = "name")
@Option(name = "reportForeignIdKey", description = "For reports that have a foreign ID column, specify the column " +
		"name.", parameter = "name")
@Option(name = "reportPathKey", description = "The report table key for storing the document path.", parameter = "name")
@Option(name = "reportStatusKey", description = "The table key for storing the report status.", parameter = "name")
@Option(name = "reportSuccessStatus", description = "The status for a successfully extracted file.", parameter =
		"value")
@Option(name = "reportFailureStatus", description = "A general failure status value to use instead of the more " +
		"specific values.", parameter = "value")
@OptionsClass(DataSourceFactory.class)
public class MySQLReport extends SQLConcurrentMap<Document, ExtractionStatus> implements Report {

	private static class ReportCodec implements SQLCodec<ExtractionStatus> {

		private final String idKey;
		private final String foreignIdKey;
		private final String pathKey;
		private final String statusKey;
		private final String successStatus;
		private final String failureStatus;

		ReportCodec(final Options<String> options) {
			this.idKey = options.get("reportIdKey").value().orElse(null);
			this.foreignIdKey = options.get("reportForeignIdKey").value().orElse(null);
			this.pathKey = options.get("reportPathKey").value().orElse("path");
			this.statusKey = options.get("reportStatusKey").value().orElse("extraction_status");
			this.successStatus = options.get("reportSuccessStatus").value().orElse(null);
			this.failureStatus = options.get("reportFailureStatus").value().orElse(null);
		}

		@Override
		public Map<String, Object> encodeKey(final Object o) {
			final Document document = (Document) o;
			final Map<String, Object> map = new HashMap<>();

			if (null != idKey) {
				map.put(idKey, document.getId());
			}

			if (null != foreignIdKey) {
				map.put(foreignIdKey, document.getForeignId());
			}

			map.put(pathKey, document.getPath().toString());
			return map;
		}

		@Override
		public Map<String, Object> encodeValue(final Object o) {
			final ExtractionStatus status = (ExtractionStatus) o;
			final Map<String, Object> map = new HashMap<>();

			if (status == ExtractionStatus.SUCCEEDED && successStatus != null) {
				map.put(statusKey, successStatus);
			} else if (failureStatus != null && status.getCode() > 0) {
				map.put(statusKey, failureStatus);
			} else {
				map.put(statusKey, status.toString());
			}

			return map;
		}

		@Override
		public ExtractionStatus decodeValue(final ResultSet rs) throws SQLException {
			final String status = rs.getString(statusKey);

			// Assume that the status is stored as an ENUM in the table, so decode as the ExtractionStatus name
			// rather than integer value. This is different to Redis codec, where integer values are stored to save
			// memory. MySQL converts ENUM strings to integers internally, so no such manual device is needed there.
			if (null == status) {
				return null;
			}

			if (null != successStatus && status.equals(successStatus)) {
				return ExtractionStatus.SUCCEEDED;
			}

			if (null != failureStatus && status.equals(failureStatus)) {
				return ExtractionStatus.UNKNOWN_ERROR;
			}

			// Return null if the value can't be decoded into an enum. This allows arbitrary values to be stored in
			// the status column.
			try {
				return ExtractionStatus.valueOf(status);
			} catch (IllegalArgumentException e) {
				return null;
			}
		}
	}

	MySQLReport(final Options<String> options) {

		// Two connections should be enough for most use-cases (one to check and one to save).
		this(new DataSourceFactory(options).withMaximumPoolSize(2).create("reportPool"), new ReportCodec(options),
				options.get("reportTable").value().orElse("documents"));
	}

	private MySQLReport(final DataSource ds, final SQLCodec<ExtractionStatus> codec, final String table) {
		super(ds, new MySQLConcurrentMapAdapter<>(codec, table));
	}

	@Override
	public void close() throws IOException {
		if (ds instanceof Closeable) {
			((Closeable) ds).close();
		}
	}
}
