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
@Option(name = "reportPathKey", description = "The report table key for storing the document path.", parameter = "name")
@Option(name = "reportStatusKey", description = "The table key for storing the report status.", parameter = "name")
@OptionsClass(DataSourceFactory.class)
public class MySQLReport extends SQLConcurrentMap<Document, ExtractionStatus> implements Report {

	private static class ReportCodec implements SQLCodec<ExtractionStatus> {

		private final String pathKey;
		private final String statusKey;

		ReportCodec(final Options<String> options) {
			this.pathKey = options.get("reportPathKey").value().orElse("path");
			this.statusKey = options.get("reportStatusKey").value().orElse("extraction_status");
		}

		@Override
		public String getUniqueKey() {
			return pathKey;
		}

		@Override
		public String getUniqueKeyValue(final Object o) {
			if (!(o instanceof Document)) {
				throw new IllegalArgumentException();
			}

			return ((Document) o).getPath().toString();
		}

		@Override
		public Map<String, Object> encodeValue(final Object o) {
			if (!(o instanceof ExtractionStatus)) {
				throw new IllegalArgumentException();
			}

			final Map<String, Object> map = new HashMap<>();

			map.put(statusKey, o.toString());
			return map;
		}

		@Override
		public ExtractionStatus decodeValue(final ResultSet rs) throws SQLException {
			return ExtractionStatus.parse(rs.getString(statusKey));
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
