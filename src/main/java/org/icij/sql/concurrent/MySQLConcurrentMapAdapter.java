package org.icij.sql.concurrent;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

import static org.apache.commons.lang.StringEscapeUtils.escapeSql;

public class MySQLConcurrentMapAdapter<K, V> implements SQLConcurrentMapAdapter<K, V> {

	private final String table;
	private final SQLCodec<V> codec;

	public MySQLConcurrentMapAdapter(final SQLCodec<V> codec, final String table) {
		this.table = escapeSql(table);
		this.codec = codec;
	}

	private int executeUpdate(final Connection c, final Object key, final V value) throws SQLException {
		final Map<String, Object> map = codec.encodeValue(value);
		final Set<String> keys = map.keySet();
		final String placeholders = String.join(",", keys.stream().map((k)-> escapeSql(k) + "=?")
				.toArray(String[]::new));

		try (final PreparedStatement q = c.prepareStatement("UPDATE " + table + " SET " +
				placeholders + " WHERE " + escapeSql(codec.getUniqueKey()) + "=?")) {
			int i = 1;

			for (String k: keys) {
				q.setObject(i++, map.get(k));
			}

			q.setString(i, codec.getUniqueKeyValue(key));

			return q.executeUpdate();
		}
	}

	private int executeInsert(final Connection c, final Object key, final V value) throws SQLException {
		final Map<String, Object> map = codec.encodeValue(value);
		final Set<String> keys = map.keySet();
		final String placeholders = String.join(",", keys.stream().map((k)-> escapeSql(k) + "=?")
				.toArray(String[]::new));

		try (final PreparedStatement q = c.prepareStatement("INSERT " + table + " SET " +
				escapeSql(codec.getUniqueKey()) + "=?, " + placeholders + ";")) {
			int i = 1;

			q.setString(i++, codec.getUniqueKeyValue(key));
			for (String k: keys) {
				q.setObject(i++, map.get(k));
			}

			return q.executeUpdate();
		}
	}

	private int executeInsertUpdate(final Connection c, final Object key, final V value) throws SQLException {
		final Map<String, Object> map = codec.encodeValue(value);
		final Set<String> keys = map.keySet();
		final String placeholders = String.join(",", keys.stream().map((k)-> escapeSql(k) + "=?")
				.toArray(String[]::new));

		try (final PreparedStatement q = c.prepareStatement("INSERT " + table + " SET " +
				escapeSql(codec.getUniqueKey()) + "=?, " + placeholders +
				" ON DUPLICATE KEY UPDATE " + placeholders + ";")) {
			int i = 1;
			final int l = keys.size();

			q.setString(i++, codec.getUniqueKeyValue(key));

			for (String k : keys) {
				q.setObject(i, map.get(k));
				q.setObject(l + i, map.get(k));
				i++;
			}

			return q.executeUpdate();
		}
	}


	private V selectForUpdate(final Connection c, final Object key) throws SQLException {
		try (final PreparedStatement q = c.prepareStatement("SELECT * FROM " + table + " WHERE " +
				escapeSql(codec.getUniqueKey()) + "=? LIMIT 1 FOR UPDATE;")) {
			q.setString(1, codec.getUniqueKeyValue(key));

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
		try (final PreparedStatement q = c.prepareStatement("DELETE FROM " + table + ";")) {
			return q.executeUpdate();
		}
	}

	@Override
	public int remove(final Connection c, final Object key, final Object value) throws SQLException {
		c.setAutoCommit(false);

		if (!value.equals(selectForUpdate(c, key))) {
			return 0;
		}

		try (final PreparedStatement q = c.prepareStatement("DELETE FROM " + table + " WHERE " +
				codec.getUniqueKey() + "=?;")) {
			q.setString(1, codec.getUniqueKeyValue(key));

			int result = q.executeUpdate();

			c.commit();
			return result;
		}
	}

	@Override
	public V remove(final Connection c, final Object key) throws SQLException {
		c.setAutoCommit(false);

		final V oldValue = selectForUpdate(c, key);

		try (final PreparedStatement q = c.prepareStatement("DELETE FROM " + table + " WHERE " +
				escapeSql(codec.getUniqueKey()) + "=?;")) {
			q.setString(1, codec.getUniqueKeyValue(key));
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
	public boolean fastPut(final Connection c, final K key, final V value) throws SQLException {
		return executeInsertUpdate(c, key, value) > 0;
	}

	@Override
	public int size(final Connection c) throws SQLException {
		try (final ResultSet rs = c.prepareStatement("SELECT COUNT(*) FROM " + table + ";").executeQuery()) {
			rs.next();
			return rs.getInt(0);
		}
	}

	@Override
	public void putAll(final Connection c, final Map<? extends K, ? extends V> m) throws SQLException {
		Map<String, Object> map = codec.encodeValue(m.entrySet().iterator().next().getValue());
		final Set<String> keys = map.keySet();
		final String placeholders = String.join(",", keys.stream().map((k)-> escapeSql(k) + "=?")
				.toArray(String[]::new));

		try (final PreparedStatement q = c.prepareStatement("INSERT " + table + " SET " +
				escapeSql(codec.getUniqueKey()) + "=?, " +
				placeholders +
				" ON DUPLICATE KEY UPDATE " + placeholders + ";")) {
			for (Map.Entry<? extends K, ? extends V> e : m.entrySet()) {
				int i = 1;
				final int l = keys.size();
				map = codec.encodeValue(e.getValue());

				q.setString(i++, codec.getUniqueKeyValue(e.getKey()));

				for (String k : keys) {
					q.setObject(i, map.get(k));
					q.setObject(l + i, map.get(k));
					i++;
				}

				q.addBatch();
			}

			q.executeBatch();
		}
	}

	@Override
	public boolean containsKey(final Connection c, final Object key) throws SQLException {
		try (final PreparedStatement q = c.prepareStatement("SELECT EXISTS(SELECT * FROM " + table +
				" WHERE " + escapeSql(codec.getUniqueKey()) + "=?);")) {
			q.setString(1, codec.getUniqueKeyValue(key));

			try (final ResultSet rs = q.executeQuery()) {
				rs.next();
				return rs.getBoolean(0);
			}
		}
	}

	@Override
	public boolean containsValue(final Connection c, final Object value) throws SQLException {
		final Map<String, Object> map = codec.encodeValue(value);

		try (final PreparedStatement q = c.prepareStatement("SELECT EXISTS(SELECT * FROM " + table + " WHERE " +
				String.join(" AND ", map.keySet().stream().map((k)-> escapeSql(k) + "=?").toArray(String[]::new)) +
				");")) {
			int i = 1;
			for (String k: map.keySet()) {
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
		try (final ResultSet rs = c.prepareStatement("SELECT EXISTS(SELECT * FROM " + table + ");").executeQuery()) {
			rs.next();
			return rs.getBoolean(0);
		}
	}

	@Override
	public V get(final Connection c, final Object key) throws SQLException {
		try (final PreparedStatement q = c.prepareStatement("SELECT * FROM " + table + " WHERE " + escapeSql(codec
				.getUniqueKey()) + "=?;")) {
			q.setString(1, codec.getUniqueKeyValue(key));

			try (final ResultSet rs = q.executeQuery()) {
				if (!rs.next()) {
					return null;
				}

				return codec.decodeValue(rs);
			}
		}
	}
}
