package org.icij.extract.mysql;

import java.sql.SQLException;
import java.util.function.Consumer;

@FunctionalInterface
public interface CheckedConsumer<T> extends Consumer<T> {

	void acceptThrows(final T t) throws SQLException;

	@Override
	default void accept(final T t) {
		try {
			acceptThrows(t);
		} catch (final SQLException e) {
			throw new RuntimeException(e);
		}
	}
}
