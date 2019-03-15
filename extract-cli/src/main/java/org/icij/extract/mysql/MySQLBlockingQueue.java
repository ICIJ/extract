package org.icij.extract.mysql;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class MySQLBlockingQueue<E> extends SQLBlockingQueue<E> {

	private final String table;
	private final SQLQueueCodec<E> codec;

	public MySQLBlockingQueue(final DataSource dataSource, final SQLQueueCodec<E> codec, final String table) {
		super(dataSource, codec, new MySQLLock(dataSource, table));
		this.table = table;
		this.codec = codec;
	}

	@Override
	public boolean remove(final Object o) {
		Objects.requireNonNull(o);

		final Map<String, Object> keys = codec.encodeKey(o);
		final Set<String> keySet = keys.keySet();

		return source.withStatementUnchecked("DELETE FROM " + table + " WHERE " +
				String.join(" AND ", keySet.stream().map(k -> k + " = ?").toArray(String[]::new)) +
				" AND " + codec.getStatusKey() + "=?;", q -> {
			int i = 1;
			for (String k: keySet) {
				q.setObject(i++, keys.get(k));
			}

			q.setString(i, codec.getWaitingStatus());
			return q.executeUpdate() > 0;
		});
	}

	@Override
	public void clear() {
		source.withStatementUnchecked("DELETE FROM " + table + " WHERE " + codec.getStatusKey() + " = ?;", q -> {
			q.setString(1, codec.getWaitingStatus());
			return q.executeUpdate();
		});
	}

	@Override
	public boolean contains(final Object o) {
		Objects.requireNonNull(o);

		final Map<String, Object> keys = codec.encodeKey(o);
		final Set<String> keySet = keys.keySet();

		return source.withStatementUnchecked("SELECT EXISTS(SELECT * FROM " + table + " WHERE " +
				String.join(" AND ", keySet.stream().map(k -> k + " = ?").toArray(String[]::new)) +
				" AND " + codec.getStatusKey() + " = ?);", q -> {
			int i = 1;
			for (String k: keySet) {
				q.setObject(i++, keys.get(k));
			}

			q.setString(i, codec.getWaitingStatus());

			try (final ResultSet rs = q.executeQuery()) {
				rs.next();
				return rs.getBoolean(1);
			}
		});
	}

	@Override
	public int size() {
		return source.withStatementUnchecked("SELECT COUNT(*) FROM " + table + " WHERE " +
				codec.getStatusKey() + " = ?;", q -> {
			q.setString(1, codec.getWaitingStatus());

			try (final ResultSet rs = q.executeQuery()) {
				rs.next();
				return rs.getInt(1);
			}
		});
	}

	@Override
	public E poll() {
		final E o;

		try (final Connection c = source.getConnection()) {
			c.setAutoCommit(false);

			try (final PreparedStatement q = c.prepareStatement("SELECT * FROM " + table + " WHERE " +
					codec.getStatusKey() + " = ? LIMIT 1 FOR UPDATE;")) {
				q.setString(1, codec.getWaitingStatus());

				try (final ResultSet rs = q.executeQuery()) {
					if (rs.next()) {
						o = codec.decodeValue(rs);
					} else {
						c.rollback();
						return null;
					}
				}
			} catch (SQLException e) {
				c.rollback();
				throw e;
			}

			final Map<String, Object> keys = codec.encodeKey(o);
			final Set<String> keySet = keys.keySet();

			try (final PreparedStatement q = c.prepareStatement("UPDATE " + table + " SET " +
					codec.getStatusKey() + " = ? WHERE " +
					String.join(" AND ", keySet.stream().map(k -> k + " = ?").toArray(String[]::new)) + ";")) {
				int i = 1;
				q.setString(i++, codec.getProcessedStatus());
				for (String k : keySet) {
					q.setObject(i++, keys.get(k));
				}

				q.executeUpdate();
			} catch (SQLException e) {
				c.rollback();
				throw e;
			}

			c.commit();
			return o;
		} catch (final SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public boolean add(final E e) {
		Objects.requireNonNull(e);

		final Map<String, Object> values = codec.encodeValue(e);
		values.putAll(codec.encodeKey(e));

		final Set<String> keys = values.keySet();

		// Warning: when queueing long values, ensure the key length is long enough.
		// Otherwise the `ON DUPLICATE KEY UPDATE` will update wrong row.
		final String s = "INSERT INTO " + table + " (" +
				String.join(", ", keys.toArray(new String[keys.size()])) +
				") VALUES(" +
				String.join(", ", keys.stream().map(k -> "?").toArray(String[]::new)) +
				") ON DUPLICATE KEY UPDATE "
				+ codec.getStatusKey() + " = ?;";

		return source.withStatementUnchecked(s, q -> {
			int i = 1;

			for (String key: keys) {
				q.setObject(i++, values.get(key));
			}

			q.setString(i, codec.getWaitingStatus());
			return q.executeUpdate() > 0;
		});
	}

	@Override
	public E peek() {
		return source.withStatementUnchecked("SELECT * FROM " + table + " WHERE " + codec.getStatusKey() +
				" = ? LIMIT 1;", q -> {
			q.setString(1, codec.getWaitingStatus());

			try (final ResultSet rs = q.executeQuery()) {
				return codec.decodeValue(rs);
			}
		});
	}

	@Override
	public Iterator<E> iterator() {
		return new MySQLIterator<>(source, codec, table);
	}
}
