package org.icij.extract.queue;

import org.icij.extract.document.TikaDocument;
import org.icij.extract.document.DocumentFactory;

import org.icij.kaxxa.sql.concurrent.MySQLBlockingQueue;
import org.icij.kaxxa.sql.SQLQueueCodec;

import org.icij.task.Options;
import org.icij.task.annotation.Option;
import org.icij.task.annotation.OptionsClass;

import javax.sql.DataSource;
import java.io.Closeable;
import java.io.IOException;

@Option(name = "queueTable", description = "The queue table Defaults to \"document_queue\".", parameter = "name")
@OptionsClass(SQLDocumentQueueCodec.class)
public class MySQLDocumentQueue extends MySQLBlockingQueue<TikaDocument> implements DocumentQueue {

	public MySQLDocumentQueue(final DataSource dataSource, final DocumentFactory factory,
	                          final Options<String> options) {
		this(dataSource, new SQLDocumentQueueCodec(factory, options),
				options.get("queueTable").value().orElse("documents"));
	}

	public MySQLDocumentQueue(final DataSource dataSource, final SQLQueueCodec<TikaDocument> codec, final String table) {
		super(dataSource, codec, table);
	}

	@Override
	public void close() throws IOException {
		if (source instanceof Closeable) {
			((Closeable) source).close();
		}
	}
}
