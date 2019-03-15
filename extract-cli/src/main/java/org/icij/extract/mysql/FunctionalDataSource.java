package org.icij.extract.mysql;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class FunctionalDataSource extends DataSourceDecorator {

	public static FunctionalDataSource cast(final DataSource dataSource) {
		if (dataSource instanceof FunctionalDataSource) {
			return (FunctionalDataSource) dataSource;
		} else {
			return new FunctionalDataSource(dataSource);
		}
	}

	public FunctionalDataSource(final DataSource dataSource) {
		super(dataSource);
	}

	public void withConnection(final CheckedConsumer<Connection> consumer) throws SQLException {
		try (final Connection c = getConnection()) {
			consumer.acceptThrows(c);
		}
	}

	public void withConnectionUnchecked(final CheckedConsumer<Connection> consumer) {
		try {
			withConnection(consumer);
		} catch (final SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public <R> R withConnection(final CheckedFunction<Connection, R> function) throws SQLException {
		try (final Connection c = getConnection()) {
			return function.applyThrows(c);
		}
	}

	public <R> R withConnectionUnchecked(final CheckedFunction<Connection, R> function) {
		try {
			return withConnection(function);
		} catch (final SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public void withStatement(final String statement, final CheckedConsumer<PreparedStatement> consumer)
			throws SQLException {
		withConnection(c -> {
			try (final PreparedStatement q = c.prepareStatement(statement)){
				consumer.acceptThrows(q);
			}
		});
	}

	public void withStatementUnchecked(final String statement, final CheckedConsumer<PreparedStatement> consumer) {
		withConnectionUnchecked(c -> {
			try (final PreparedStatement q = c.prepareStatement(statement)){
				consumer.acceptThrows(q);
			}
		});
	}

	public <R> R withStatement(final String statement, final CheckedFunction<PreparedStatement, R> function)
			throws SQLException {
		return withConnection(c -> {
			try (final PreparedStatement q = c.prepareStatement(statement)) {
				return function.applyThrows(q);
			}
		});
	}

	public <R> R withStatementUnchecked(final String statement, final CheckedFunction<PreparedStatement, R> function) {
		return withConnectionUnchecked(c -> {
			try (final PreparedStatement q = c.prepareStatement(statement)) {
				return function.applyThrows(q);
			}
		});
	}
}
