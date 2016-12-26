package org.icij.sql.concurrent;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

public class MySQLConcurrentMapAdapter<K, V> implements SQLConcurrentMapAdapter<K, V> {

	private final String table;
	private final SQLCodec<V> codec;

	public MySQLConcurrentMapAdapter(final SQLCodec<V> codec, final String table) {
		this.table = table;
		this.codec = codec;
	}

	private int executeUpdate(final Connection c, final Object key, final V value) throws SQLException {
		final Map<String, Object> map = codec.encodeValue(value);
		final Set<String> keys = map.keySet();
		final String[] placeholders = new String[keys.size()];

		Arrays.fill(placeholders, "?=?");

		try (final PreparedStatement q = c.prepareStatement("UPDATE ? SET ?=?, " + String.join(",", placeholders) +
				" WHERE ?=?")) {
			int i = 1;

			q.setString(i++, table);
			q.setString(i++, codec.getUniqueKey());
			q.setString(i++, codec.getUniqueKeyValue(key));

			for (String k: keys) {
				q.setString(i++, k);
				q.setObject(i++, map.get(k));
			}

			q.setString(i++, codec.getUniqueKey());
			q.setString(i, codec.getUniqueKeyValue(key));

			return q.executeUpdate();
		}
	}

	private int executeInsert(final Connection c, final Object key, final V value) throws SQLException {
		final Map<String, Object> map = codec.encodeValue(value);
		final Set<String> keys = map.keySet();
		final String[] placeholders = new String[keys.size()];

		Arrays.fill(placeholders, "?=?");

		try (final PreparedStatement q = c.prepareStatement("INSERT ? SET ?=?, " + String.join(",", placeholders) +
				";")) {
			int i = 1;

			q.setString(i++, table);
			q.setString(i++, codec.getUniqueKey());
			q.setString(i++, codec.getUniqueKeyValue(key));

			for (String k: keys) {
				q.setString(i++, k);
				q.setObject(i++, map.get(k));
			}

			return q.executeUpdate();
		}
	}

	private V selectForUpdate(final Connection c, final Object key) throws SQLException {
		try (final PreparedStatement q = c.prepareStatement("SELECT * FROM ? WHERE ?=? LIMIT 1 FOR UPDATE;")) {
			q.setString(1, table);
			q.setString(2, codec.getUniqueKey());
			q.setString(3, codec.getUniqueKeyValue(key));

			try (final ResultSet rs = q.executeQuery()) {
				if (!rs.next()) {
					return null;
				}

				return codec.decodeValue(rs);
			}
		}
	}

	@Override
	public int replace(final Connection c, final K key, final V oldValue, final V newValue) throws SQLException {
		c.setAutoCommit(false);

		if (!oldValue.equals(selectForUpdate(c, key))) {
			return 0;
		}

		final int result = executeUpdate(c, key, newValue);

		c.commit();
		return result;
	}

	@Override
	public V replace(final Connection c, final K key, final V value) throws SQLException {
		c.setAutoCommit(false);

		final V oldValue = selectForUpdate(c, key);

		executeUpdate(c, key, value);
		c.commit();

		return oldValue;
	}

	@Override
	public int clear(final Connection c) throws SQLException {
		try (final PreparedStatement q = c.prepareStatement("DELETE FROM ?;")) {
			q.setString(1, table);

			return q.executeUpdate();
		}
	}

	@Override
	public int remove(final Connection c, final Object key, final Object value) throws SQLException {
		c.setAutoCommit(false);

		if (!value.equals(selectForUpdate(c, key))) {
			return 0;
		}

		try (final PreparedStatement q = c.prepareStatement("DELETE FROM ? WHERE ?=?;")) {
			int result = q.executeUpdate();

			c.commit();
			return result;
		}
	}

	@Override
	public V remove(final Connection c, final Object key) throws SQLException {
		c.setAutoCommit(false);

		final V oldValue = selectForUpdate(c, key);

		try (final PreparedStatement q = c.prepareStatement("DELETE FROM ? WHERE ?=?;")) {
			q.setString(1, table);
			q.setString(2, codec.getUniqueKey());
			q.setString(3, codec.getUniqueKeyValue(key));
			q.executeUpdate();

			c.commit();
			return oldValue;
		}
	}

	@Override
	public V put(final Connection c, final K key, final V value) throws SQLException {
		c.setAutoCommit(false);

		final V oldValue = selectForUpdate(c, key);

		if (null == oldValue) {
			executeUpdate(c, key, value);
		} else {
			executeInsert(c, key, value);
		}

		c.commit();
		return oldValue;
	}

	@Override
	public V putIfAbsent(final Connection c, final K key, final V value) throws SQLException {
		c.setAutoCommit(false);

		final V oldValue = selectForUpdate(c, key);

		if (null == oldValue) {
			executeInsert(c, key, value);
		}

		return oldValue;
	}

	@Override
	public int size(final Connection c) throws SQLException {
		try (final PreparedStatement q = c.prepareStatement("SELECT COUNT(*) FROM ?;")) {
			q.setString(1, table);

			try (final ResultSet rs = q.executeQuery()) {
				rs.next();
				return rs.getInt(0);
			}
		}
	}

	@Override
	public void putAll(final Connection c, final Map<? extends K, ? extends V> m) throws SQLException {
		Map<String, Object> map = codec.encodeValue(m.entrySet().iterator().next().getValue());
		final String[] placeholders = new String[map.size()];

		Arrays.fill(placeholders, "?=?");

		try (final PreparedStatement q = c.prepareStatement("INSERT ? SET ?=?, " + String.join(",", placeholders) +
				" ON DUPLICATE KEY UPDATE " + String.join(",", placeholders) + ";")) {
			for (Map.Entry<? extends K, ? extends V> e : m.entrySet()) {

				q.setString(1, table);

				int i = 2;
				final int l = placeholders.length;
				map = codec.encodeValue(e.getValue());

				for (String k : map.keySet()) {
					q.setString(i++, codec.getUniqueKey());
					q.setString(i++, codec.getUniqueKeyValue(e.getKey()));

					q.setString(i, k);
					q.setObject(i + 1, map.get(k));
					q.setString(l + i, k);
					q.setObject(l + i + 1, map.get(k));
				}

				q.addBatch();
			}

			q.executeBatch();
		}
	}

	@Override
	public boolean containsKey(final Connection c, final Object key) throws SQLException {
		try (final PreparedStatement q = c.prepareStatement("SELECT EXISTS(SELECT * FROM ? WHERE ?=?);")) {
			q.setString(1, table);
			q.setString(2, codec.getUniqueKey());
			q.setString(3, codec.getUniqueKeyValue(key));

			try (final ResultSet rs = q.executeQuery()) {
				rs.next();
				return rs.getBoolean(0);
			}
		}
	}

	@Override
	public boolean containsValue(final Connection c, final Object value) throws SQLException {
		final Map<String, Object> map = codec.encodeValue(value);
		final String[] placeholders = new String[map.size()];

		Arrays.fill(placeholders, "?=?");

		try (final PreparedStatement q = c.prepareStatement("SELECT EXISTS(SELECT * FROM ? WHERE " +
				String.join(" AND ", placeholders) + ");")) {
			q.setString(1, table);

			int i = 2;
			for (String k: map.keySet()) {
				q.setString(i++, k);
				q.setObject(i++, map.get(k));
			}

			try (final ResultSet rs = q.executeQuery()) {
				rs.next();
				return rs.getBoolean(0);
			}
		}
	}

	@Override
	public boolean isEmpty(final Connection c) throws SQLException {
		try (final PreparedStatement q = c.prepareStatement("SELECT EXISTS(SELECT * FROM ?);")) {
			q.setString(1, table);

			try (final ResultSet rs = q.executeQuery()) {
				rs.next();
				return rs.getBoolean(0);
			}
		}
	}

	@Override
	public V get(final Connection c, final Object key) throws SQLException {
		try (final PreparedStatement q = c.prepareStatement("SELECT * FROM ? WHERE ?=?;")) {
			q.setString(1, table);
			q.setString(2, codec.getUniqueKey());
			q.setString(3, codec.getUniqueKeyValue(key));

			try (final ResultSet rs = q.executeQuery()) {
				if (!rs.next()) {
					return null;
				}

				return codec.decodeValue(rs);
			}
		}
	}
}
