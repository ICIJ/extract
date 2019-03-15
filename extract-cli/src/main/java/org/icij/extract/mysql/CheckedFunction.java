package org.icij.extract.mysql;

import java.sql.SQLException;
import java.util.function.Function;

@FunctionalInterface
public interface CheckedFunction<T, R> extends Function<T, R> {

	R applyThrows(final T t) throws SQLException;

	@Override
	default R apply(final T t) {
		try {
			return applyThrows(t);
		} catch (final SQLException e) {
			throw new RuntimeException(e);
		}
	}
}
