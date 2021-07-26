package org.icij.extract.mysql;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.util.Iterator;

public class MySQLIterator<E> implements Iterator<E> {

	private final FunctionalDataSource source;
	private final SQLCodec<E> codec;
	private final String table;

	private int i = 0;
	private E buffer = null;
	private boolean ended = false;

	public MySQLIterator(final DataSource source, final SQLCodec<E> codec, final String table) {
		this.source = FunctionalDataSource.cast(source);
		this.codec = codec;
		this.table = table;
	}

	private void load() {
		buffer = source.withStatementUnchecked("SELECT * FROM " + table + " LIMIT " + i++ + ", 1;", q -> {
			final ResultSet rs = q.executeQuery();

			rs.next();
			return codec.decodeValue(rs);
		});
	}

	private void ensureLoaded() {
		if (null == buffer && !ended) {
			load();
		}
	}

	@Override
	public E next() {
		ensureLoaded();

		final E next = buffer;
		buffer = null;
		return next;
	}

	@Override
	public boolean hasNext() {
		ensureLoaded();
		ended = null == buffer;
		return !ended;
	}
}
