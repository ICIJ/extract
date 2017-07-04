package org.icij.extract.report;

import org.icij.extract.document.Document;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.mysql.DataSourceFactory;

import org.icij.kaxxa.sql.concurrent.MySQLConcurrentMap;
import org.icij.kaxxa.sql.SQLMapCodec;

import org.icij.task.Options;
import org.icij.task.annotation.Option;
import org.icij.task.annotation.OptionsClass;

import javax.sql.DataSource;
import java.io.Closeable;
import java.io.IOException;

@Option(name = "reportTable", description = "The report table. Defaults to \"document_report\".", parameter = "name")
@OptionsClass(DataSourceFactory.class)
@OptionsClass(SQLReportCodec.class)
public class MySQLReportMap extends MySQLConcurrentMap<Document, Report> implements ReportMap {

	public MySQLReportMap(final DocumentFactory factory, final Options<String> options) {

		// Two connections should be enough for most use-cases (one to check and one to save).
		this(new DataSourceFactory(options).withMaximumPoolSize(2).create("reportPool"), new SQLReportCodec(factory,
						options), options.get("reportTable").value().orElse("documents"));
	}

	public MySQLReportMap(final DataSource dataSource, final SQLMapCodec<Document, Report> codec, final String table) {
		super(dataSource, codec, table);
	}

	@Override
	public void close() throws IOException {
		if (dataSource instanceof Closeable) {
			((Closeable) dataSource).close();
		}
	}
}
