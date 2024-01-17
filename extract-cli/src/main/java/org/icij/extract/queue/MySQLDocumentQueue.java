package org.icij.extract.queue;

import org.icij.extract.document.DocumentFactory;
import org.icij.extract.mysql.MySQLBlockingQueue;
import org.icij.extract.mysql.SQLQueueCodec;
import org.icij.task.Options;
import org.icij.task.annotation.Option;
import org.icij.task.annotation.OptionsClass;

import javax.sql.DataSource;
import java.io.Closeable;
import java.io.IOException;

@Option(name = "queueTable", description = "The queue table Defaults to \"document_queue\".", parameter = "name")
@OptionsClass(SQLDocumentQueueCodec.class)
public class MySQLDocumentQueue<T> extends MySQLBlockingQueue<T> implements DocumentQueue<T> {

	public MySQLDocumentQueue(final DataSource dataSource, final DocumentFactory factory,
	                          final Options<String> options, Class<T> clazz) {
		this(dataSource, new SQLDocumentQueueCodec<>(factory, options, clazz),
				options.get("queueTable").value().orElse("documents"));
	}

	public MySQLDocumentQueue(final DataSource dataSource, final SQLQueueCodec<T> codec, final String table) {
		super(dataSource, codec, table);
	}

	@Override
	public void close() throws IOException {
		if (source instanceof Closeable) {
			((Closeable) source).close();
		}
	}

	@Override
	public String getName() {
		return table;
	}
}
