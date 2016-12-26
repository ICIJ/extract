package org.icij.sql.concurrent;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
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
	public int delete(final Connection c, final Object o) throws SQLException {
		try (final PreparedStatement q = c.prepareStatement("DELETE FROM ? WHERE ?=? AND ?=?;")) {
			q.setString(1, table);
			q.setString(2, codec.getUniqueKey());
			q.setString(3, codec.getUniqueKeyValue(o));
			q.setString(4, codec.getStatusKey());
			q.setString(5, codec.getWaitingStatus());
			return q.executeUpdate();
		}
	}

	@Override
	public int deleteAll(final Connection c) throws SQLException {
		try (final PreparedStatement q = c.prepareStatement("DELETE FROM ? WHERE ?=?;" + "")) {
			q.setString(1, table);
			q.setString(2, codec.getStatusKey());
			q.setString(3, codec.getWaitingStatus());
			return q.executeUpdate();
		}
	}

	@Override
	public boolean exists(final Connection c, final Object o) throws SQLException {
		try (final PreparedStatement q = c.prepareStatement("SELECT EXISTS(SELECT * FROM ? WHERE ?=? AND ?=?);")) {
			q.setString(1, table);
			q.setString(2, codec.getUniqueKey());
			q.setString(3, codec.getUniqueKeyValue(o));
			q.setString(4, codec.getStatusKey());
			q.setString(5, codec.getWaitingStatus());

			try (final ResultSet rs = q.executeQuery()) {
				rs.next();
				return rs.getBoolean(0);
			}
		}
	}

	@Override
	public int count(final Connection c) throws SQLException {
		try (final PreparedStatement q = c.prepareStatement("SELECT COUNT(*) FROM ? WHERE ?=?;")) {
			q.setString(1, table);
			q.setString(2, codec.getStatusKey());
			q.setString(3, codec.getWaitingStatus());

			try (final ResultSet rs = q.executeQuery()) {
				rs.next();
				return rs.getInt(0);
			}
		}
	}

	@Override
	public T shift(final Connection c) throws SQLException {
		c.setAutoCommit(false);

		try (final PreparedStatement q = c.prepareStatement("SELECT * FROM ? WHERE ?=? LIMIT 1 FOR UPDATE;")) {
			q.setString(1, table);
			q.setString(2, codec.getStatusKey());
			q.setString(3, codec.getWaitingStatus());

			final T o;

			try (final ResultSet rs = q.executeQuery()) {
				if (rs.next()) {
					o = codec.decodeValue(rs);
				} else {
					return null;
				}
			}

			try (final PreparedStatement qu = c.prepareStatement("UPDATE ? SET ?=? WHERE ?=?")) {
				qu.setString(1, table);
				qu.setString(2, codec.getStatusKey());
				qu.setString(3, codec.getProcessedStatus());
				qu.setString(4, codec.getUniqueKey());
				qu.setString(5, codec.getUniqueKeyValue(o));

				qu.executeUpdate();
			}

			c.commit();
			return o;
		} catch (SQLException e) {
			c.rollback();
			throw e;
		}
	}

	@Override
	public int insert(final Connection c, final T e) throws SQLException {
		final Map<String, Object> map = codec.encodeValue(e);
		final Set<String> keys = map.keySet();
		final String[] placeholders = new String[keys.size()];
		final StringBuilder s = new StringBuilder("INSERT INTO ? (");

		Arrays.fill(placeholders, "?");
		s.append(String.join(", ", placeholders));
		s.append(") VALUES(");
		s.append(String.join(", ", placeholders));
		s.append(");");

		try (final PreparedStatement q = c.prepareStatement(s.toString())) {
			int i = 1;
			int l = placeholders.length;
			for (String key: keys) {
				q.setString(i, key);
				q.setObject(l + i++, map.get(key));
			}

			return q.executeUpdate();
		}
	}

	@Override
	public T selectFirst(final Connection c) throws SQLException {
		try (final PreparedStatement q = c.prepareStatement("SELECT * FROM ? WHERE ?=? LIMIT 1;")) {
			q.setString(1, table);
			q.setString(2, codec.getStatusKey());
			q.setString(3, codec.getWaitingStatus());

			try (final ResultSet rs = q.executeQuery()) {
				return codec.decodeValue(rs);
			}
		}
	}
}
