package org.icij.extract.mysql;

import com.mysql.cj.exceptions.MysqlErrorNumbers;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class MySQLSet<E> extends SQLSet<E> {

	private String table;

	public MySQLSet(final DataSource dataSource, final SQLCodec<E> codec, final String table) {
		super(dataSource, codec);
		this.table = table;
	}

	@Override
	public int size() {
		return source.withStatementUnchecked("SELECT COUNT(*) FROM " + table + ";", q -> {
			try (final ResultSet rs = q.executeQuery()) {
				rs.next();
				return rs.getInt(1);
			}
		});
	}

	@Override
	public boolean isEmpty() {
		return source.withStatementUnchecked("SELECT EXISTS(SELECT * FROM " + table + ");", q -> {
			final ResultSet rs = q.executeQuery();

			rs.next();
			return rs.getBoolean(1);
		});
	}

	@Override
	public boolean contains(final Object o) {
		Objects.requireNonNull(o);

		final Map<String, Object> keys = codec.encodeKey(o);
		final Set<String> keySet = keys.keySet();

		final String sql = "SELECT EXISTS(SELECT * FROM " + table + " WHERE " +
				String.join(" AND ", keySet.stream().map(k -> k + " = ?").toArray(String[]::new)) + ";";

		return source.withStatementUnchecked(sql, q -> {
			int i = 1;
			for (String k: keySet) {
				q.setObject(i++, keys.get(k));
			}

			try (final ResultSet rs = q.executeQuery()) {
				rs.next();
				return rs.getBoolean(1);
			}
		});
	}

	@Override
	public boolean add(final E e) {
		Objects.requireNonNull(e);

		final Map<String, Object> values = codec.encodeValue(e);
		values.putAll(codec.encodeKey(e));

		final Set<String> keys = values.keySet();

		final String sql = "INSERT INTO " + table + " (" +
				String.join(", ", keys.toArray(new String[keys.size()])) +
				") VALUES(" +
				String.join(", ", Collections.nCopies(keys.size(), "?")) +
				");";

		return source.withStatementUnchecked(sql, q -> {
			int i = 1;

			for (String key: keys) {
				q.setObject(i++, values.get(key));
			}

			try {
				return q.executeUpdate() > 0;
			} catch (final SQLException exc) {
				if (exc.getErrorCode() == MysqlErrorNumbers.ER_DUP_ENTRY) {
					return false;
				}

				throw exc;
			}
		});
	}

	@Override
	public boolean remove(final Object o) {
		Objects.requireNonNull(o);

		final Map<String, Object> values = codec.encodeKey(o);
		final Set<String> keys = values.keySet();

		final String sql = "DELETE FROM " + table + " WHERE " +
				String.join(" AND ", keys.stream().map(k -> k + " = ?").toArray(String[]::new)) +
				";";

		return source.withStatementUnchecked(sql, q -> {
			int i = 1;

			for (String k: keys) {
				q.setObject(i++, values.get(k));
			}

			return q.executeUpdate() > 0;
		});
	}

	@Override
	public void clear() {
		source.withStatementUnchecked("DELETE FROM " + table + ";",
				(CheckedFunction<PreparedStatement, Integer>) PreparedStatement::executeUpdate);
	}

	@Override
	public Iterator<E> iterator() {
		return new MySQLIterator<>(source, codec, table);
	}
}
