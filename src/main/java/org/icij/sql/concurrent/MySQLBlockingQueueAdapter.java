package org.icij.sql.concurrent;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

public class MySQLBlockingQueueAdapter<T> implements SQLBlockingQueueAdapter<T> {

	private final String table;
	private final SQLQueueCodec<T> codec;

	public MySQLBlockingQueueAdapter(final SQLQueueCodec<T> codec, final String table) {
		this.table = table;
		this.codec = codec;
	}

	@Override
	public int remove(final Connection c, final Object o) throws SQLException {
		final Map<String, Object> keys = codec.encodeKey(o);
		final Set<String> keySet = keys.keySet();

		try (final PreparedStatement q = c.prepareStatement("DELETE FROM " + table + " WHERE " +
				String.join(" AND ", keySet.stream().map(k -> k + " = ?").toArray(String[]::new)) +
				" AND " + codec.getStatusKey() + "=?;")) {
			int i = 1;
			for (String k: keySet) {
				q.setObject(i++, keys.get(k));
			}

			q.setString(i, codec.getWaitingStatus());
			return q.executeUpdate();
		}
	}

	@Override
	public int clear(final Connection c) throws SQLException {
		try (final PreparedStatement q = c.prepareStatement("DELETE FROM " + table + " WHERE " +
				codec.getStatusKey() + " = ?;")) {
			q.setString(1, codec.getWaitingStatus());
			return q.executeUpdate();
		}
	}

	@Override
	public boolean contains(final Connection c, final Object o) throws SQLException {
		final Map<String, Object> keys = codec.encodeKey(o);
		final Set<String> keySet = keys.keySet();

		try (final PreparedStatement q = c.prepareStatement("SELECT EXISTS(SELECT * FROM " + table + " WHERE " +
				String.join(" AND ", keySet.stream().map(k -> k + " = ?").toArray(String[]::new)) +
				" AND " + codec.getStatusKey() + " = ?);")) {
			int i = 1;
			for (String k: keySet) {
				q.setObject(i++, keys.get(k));
			}

			q.setString(i, codec.getWaitingStatus());

			try (final ResultSet rs = q.executeQuery()) {
				rs.next();
				return rs.getBoolean(0);
			}
		}
	}

	@Override
	public int size(final Connection c) throws SQLException {
		try (final PreparedStatement q = c.prepareStatement("SELECT COUNT(*) FROM " + table + " WHERE " +
				codec.getStatusKey() + " = ?;")) {
			q.setString(1, codec.getWaitingStatus());

			try (final ResultSet rs = q.executeQuery()) {
				rs.next();
				return rs.getInt(0);
			}
		}
	}

	@Override
	public T poll(final Connection c) throws SQLException {
		final T o;
		c.setAutoCommit(false);

		try (final PreparedStatement q = c.prepareStatement("SELECT * FROM " + table + " WHERE " +
				codec.getStatusKey() + " = ? LIMIT 1 FOR UPDATE;")) {
			q.setString(1, codec.getWaitingStatus());

			try (final ResultSet rs = q.executeQuery()) {
				if (rs.next()) {
					o = codec.decodeValue(rs);
				} else {
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
			for (String k: keySet) {
				q.setObject(i++, keys.get(k));
			}

			q.executeUpdate();
		} catch (SQLException e) {
			c.rollback();
			throw e;
		}

		c.commit();
		return o;
	}

	@Override
	public int add(final Connection c, final T e) throws SQLException {
		final Map<String, Object> values = codec.encodeValue(e);
		values.putAll(codec.encodeKey(e));

		final Set<String> keys = values.keySet();

		final String s = "INSERT INTO " + table + " (" +
				String.join(", ", keys.toArray(new String[keys.size()])) +
				") VALUES(" +
				String.join(", ", keys.stream().map(k -> "?").toArray(String[]::new)) + ") ON DUPLICATE KEY UPDATE "
		+ codec.getStatusKey() + " = ?;";

		try (final PreparedStatement q = c.prepareStatement(s)) {
			int i = 1;

			for (String key: keys) {
				q.setObject(i++, values.get(key));
			}

			q.setString(i, codec.getWaitingStatus());
			return q.executeUpdate();
		} catch (SQLException ex) {
			System.err.println(s);
			throw ex;
		}
	}

	@Override
	public T peek(final Connection c) throws SQLException {
		try (final PreparedStatement q = c.prepareStatement("SELECT * FROM " + table + " WHERE " +
				codec.getStatusKey() + " = ? LIMIT 1;")) {
			q.setString(1, codec.getWaitingStatus());

			try (final ResultSet rs = q.executeQuery()) {
				return codec.decodeValue(rs);
			}
		}
	}
}
