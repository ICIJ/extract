package org.icij.extract.mysql;

import javax.sql.DataSource;
import java.util.Set;

abstract class SQLSet<E> extends SQLCollection<E> implements Set<E> {

	SQLSet(final DataSource dataSource, final SQLCodec<E> codec) {
		super(dataSource, codec);
	}
}
