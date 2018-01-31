package org.icij.extract.report;

import org.icij.extract.document.TikaDocument;
import org.icij.extract.document.DocumentFactory;

import org.icij.kaxxa.sql.concurrent.MySQLConcurrentMap;
import org.icij.kaxxa.sql.SQLMapCodec;

import org.icij.task.Options;
import org.icij.task.annotation.Option;
import org.icij.task.annotation.OptionsClass;

import javax.sql.DataSource;
import java.io.Closeable;
import java.io.IOException;

@Option(name = "reportTable", description = "The report table. Defaults to \"document_report\".", parameter = "name")
@OptionsClass(SQLReportCodec.class)
public class MySQLReportMap extends MySQLConcurrentMap<TikaDocument, Report> implements ReportMap {

	public MySQLReportMap(final DataSource dataSource, final DocumentFactory factory, final Options<String> options) {
		this(dataSource, new SQLReportCodec(factory, options),
				options.get("reportTable").value().orElse("documents"));
	}

	public MySQLReportMap(final DataSource dataSource, final SQLMapCodec<TikaDocument, Report> codec, final String table) {
		super(dataSource, codec, table);
	}

	@Override
	public void close() throws IOException {
		if (dataSource instanceof Closeable) {
			((Closeable) dataSource).close();
		}
	}
}
