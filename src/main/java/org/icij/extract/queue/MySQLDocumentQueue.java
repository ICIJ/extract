package org.icij.extract.queue;

import org.icij.extract.document.Document;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.mysql.DataSourceFactory;

import org.icij.kaxxa.sql.concurrent.MySQLBlockingQueue;
import org.icij.kaxxa.sql.concurrent.SQLQueueCodec;

import org.icij.task.Options;
import org.icij.task.annotation.Option;
import org.icij.task.annotation.OptionsClass;

import javax.sql.DataSource;
import java.io.Closeable;
import java.io.IOException;

@Option(name = "queueTable", description = "The queue table Defaults to \"document_queue\".", parameter = "name")
@OptionsClass(DataSourceFactory.class)
@OptionsClass(SQLDocumentQueueCodec.class)
public class MySQLDocumentQueue extends MySQLBlockingQueue<Document> implements DocumentQueue {

	public MySQLDocumentQueue(final DocumentFactory factory, final Options<String> options) {

		// The queue should never need more than two connections per process: one to add and one to poll.
		this(new DataSourceFactory(options).withMaximumPoolSize(2).create("queuePool"),
				new SQLDocumentQueueCodec(factory, options), options.get("queueTable").value().orElse("documents"));
	}

	public MySQLDocumentQueue(final DataSource dataSource, final SQLQueueCodec<Document> codec, final String table) {
		super(dataSource, codec, table);
	}

	@Override
	public void close() throws IOException {
		if (dataSource instanceof Closeable) {
			((Closeable) dataSource).close();
		}
	}
}
