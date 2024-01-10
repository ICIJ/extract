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
import java.nio.file.Path;

@Option(name = "queueTable", description = "The queue table Defaults to \"document_queue\".", parameter = "name")
@OptionsClass(SQLDocumentQueueCodec.class)
public class MySQLDocumentQueue extends MySQLBlockingQueue<Path> implements DocumentQueue<Path> {

	public MySQLDocumentQueue(final DataSource dataSource, final DocumentFactory factory,
	                          final Options<String> options) {
		this(dataSource, new SQLDocumentQueueCodec(factory, options),
				options.get("queueTable").value().orElse("documents"));
	}

	public MySQLDocumentQueue(final DataSource dataSource, final SQLQueueCodec<Path> codec, final String table) {
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
