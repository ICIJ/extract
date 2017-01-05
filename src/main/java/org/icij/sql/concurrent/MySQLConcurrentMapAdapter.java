package org.icij.sql.concurrent;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

public class MySQLConcurrentMapAdapter<K, V> implements SQLConcurrentMapAdapter<K, V> {

	private final String table;
	private final SQLCodec<V> codec;

	public MySQLConcurrentMapAdapter(final SQLCodec<V> codec, final String table) {
		this.table = table;
		this.codec = codec;
	}

	private int executeUpdate(final Connection c, final K key, final V value) throws SQLException {
		final Map<String, Object> values = codec.encodeValue(value);
		final Set<String> valuesKeySet = values.keySet();

		final Map<String, Object> keys = codec.encodeKey(key);
		final Set<String> keysKeySet = keys.keySet();

		try (final PreparedStatement q = c.prepareStatement("UPDATE " + table + " SET " +
				String.join(",", valuesKeySet.stream().map(k -> k + " = ?").toArray(String[]::new)) +
				" WHERE " +
				String.join(" AND ", keysKeySet.stream().map(k -> k + " = ?").toArray(String[]::new)) + ";")) {
			int i = 1;

			for (String k: valuesKeySet) {
				q.setObject(i++, values.get(k));
			}

			for (String k: keysKeySet) {
				q.setObject(i++, keys.get(k));
			}

			return q.executeUpdate();
		}
	}

	private V selectForUpdate(final Connection c, final Object key) throws SQLException {
		final Map<String, Object> keys = codec.encodeKey(key);
		final Set<String> keySet = keys.keySet();

		try (final PreparedStatement q = c.prepareStatement("SELECT * FROM " + table + " WHERE " +
				String.join(" AND ", keySet.stream().map(k -> k + " = ?").toArray(String[]::new)) +
				" LIMIT 1 FOR UPDATE;")) {
			int i = 1;
			for (String k: keySet) {
				q.setObject(i++, keys.get(k));
			}

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
		final int result;
		c.setAutoCommit(false);

		try {
			if (!oldValue.equals(selectForUpdate(c, key))) {
				c.rollback();
				return 0;
			}

			result = executeUpdate(c, key, newValue);
		} catch (SQLException e) {
			c.rollback();
			throw e;
		}

		c.commit();
		return result;
	}

	@Override
	public V replace(final Connection c, final K key, final V value) throws SQLException {
		final V oldValue;
		c.setAutoCommit(false);

		try {
			oldValue = selectForUpdate(c, key);
			executeUpdate(c, key, value);
		} catch (SQLException e) {
			c.rollback();
			throw e;
		}

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
		final int result;
		c.setAutoCommit(false);

		try {
			if (!value.equals(selectForUpdate(c, key))) {
				c.rollback();
				return 0;
			}
		} catch (SQLException e) {
			c.rollback();
			throw e;
		}

		final Map<String, Object> values = codec.encodeKey(key);
		values.putAll(codec.encodeValue(value));

		final Set<String> keySet = values.keySet();

		try (final PreparedStatement q = c.prepareStatement("DELETE FROM " + table + " WHERE " +
				String.join(" AND ", keySet.stream().map(k -> k + " = ?").toArray(String[]::new)) + ";")) {
			int i = 1;
			for (String k: keySet) {
				q.setObject(i++, values.get(k));
			}

			result = q.executeUpdate();
		} catch (SQLException e) {
			c.rollback();
			throw e;
		}

		c.commit();
		return result;
	}

	@Override
	public V remove(final Connection c, final Object key) throws SQLException {
		final V oldValue;
		c.setAutoCommit(false);

		try {
			oldValue = selectForUpdate(c, key);
		} catch (SQLException e) {
			c.rollback();
			throw e;
		}

		final Map<String, Object> keys = codec.encodeKey(key);
		final Set<String> keySet = keys.keySet();

		try (final PreparedStatement q = c.prepareStatement("DELETE FROM " + table + " WHERE " +
				String.join(" AND ", keySet.stream().map(k -> k + " = ?").toArray(String[]::new)) + ";")) {
			int i = 1;
			for (String k: keySet) {
				q.setObject(i++, keys.get(k));
			}

			q.executeUpdate();
		} catch (SQLException e) {
			c.rollback();
			throw e;
		}

		c.commit();
		return oldValue;
	}

	@Override
	public V put(final Connection c, final K key, final V value) throws SQLException {
		final V oldValue;
		c.setAutoCommit(false);

		try {
			oldValue = selectForUpdate(c, key);
		} catch (SQLException e) {
			c.rollback();
			throw e;
		}

		try {
			if (null == oldValue) {
				executeUpdate(c, key, value);
			} else {
				fastPut(c, key, value);
			}
		} catch (SQLException e) {
			c.rollback();
			throw e;
		}

		c.commit();
		return oldValue;
	}

	@Override
	public V putIfAbsent(final Connection c, final K key, final V value) throws SQLException {
		final V oldValue = get(c, key);

		if (null != oldValue) {
			return oldValue;
		}

		final Map<String, Object> values = codec.encodeValue(value);
		values.putAll(codec.encodeKey(key));

		// There's a race condition here. An exception might be thrown if a record with the same keys is inserted
		// between the call to #get(...) and this point.
		try (final PreparedStatement q = c.prepareStatement("INSERT " + table + " SET " +
			String.join(",", values.keySet().stream().map(k -> k + " = ?").toArray(String[]::new)) + ";")) {

			int i = 1;
			for (String k: values.keySet()) {
				q.setObject(i++, values.get(k));
			}

			q.executeUpdate();
		}

		return null;
	}

	@Override
	public boolean fastPut(final Connection c, final K key, final V value) throws SQLException {
		final Map<String, Object> values = codec.encodeValue(value);
		values.putAll(codec.encodeKey(key));

		final Set<String> keySet = values.keySet();

		final String placeholders = String.join(", ", keySet.stream().map(k -> k + " = ?")
				.toArray(String[]::new));

		try (final PreparedStatement q = c.prepareStatement("INSERT " + table + " SET " + placeholders +
				" ON DUPLICATE KEY UPDATE " + placeholders + ";")) {
			int i = 1;
			final int l = keySet.size();

			for (String k : keySet) {
				q.setObject(i, values.get(k));
				q.setObject(l + i, values.get(k));
				i++;
			}

			return q.executeUpdate() > 0;
		}
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
		for (Map.Entry<? extends K, ? extends V> e : m.entrySet()) {
			final Map<String, Object> values = codec.encodeValue(e.getValue());

			// Merge in the keys.
			values.putAll(codec.encodeKey(e.getKey()));

			final Set<String> keySet = values.keySet();

			final String placeholders = String.join(", ", keySet.stream().map(k -> k + " = ?")
					.toArray(String[]::new));

			try (final PreparedStatement q = c.prepareStatement("INSERT " + table + " SET " + placeholders +
					" ON DUPLICATE KEY UPDATE " + placeholders + ";")) {

				int i = 1;
				final int l = values.size();

				for (String k : keySet) {
					q.setObject(i, values.get(k));
					q.setObject(l + i, values.get(k));
					i++;
				}

				q.executeUpdate();
			}
		}
	}

	@Override
	public boolean containsKey(final Connection c, final Object key) throws SQLException {
		final Map<String, Object> keys = codec.encodeKey(key);
		final Set<String> keySet = keys.keySet();

		try (final PreparedStatement q = c.prepareStatement("SELECT EXISTS(SELECT * FROM " + table + " WHERE " +
				String.join(" AND ", keySet.stream().map(k -> k + " = ?").toArray(String[]::new)) + ";")) {
			int i = 1;
			for (String k: keySet) {
				q.setObject(i++, keys.get(k));
			}

			try (final ResultSet rs = q.executeQuery()) {
				rs.next();
				return rs.getBoolean(0);
			}
		}
	}

	@Override
	public boolean containsValue(final Connection c, final Object value) throws SQLException {
		final Map<String, Object> values = codec.encodeValue(value);
		final Set<String> keySet = values.keySet();

		try (final PreparedStatement q = c.prepareStatement("SELECT EXISTS(SELECT * FROM " + table + " WHERE " +
				String.join(" AND ", keySet.stream().map((k)-> k + " = ?").toArray(String[]::new)) + ");")) {
			int i = 1;
			for (String k: keySet) {
				q.setObject(i++, values.get(k));
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
		final Map<String, Object> keys = codec.encodeKey(key);
		final Set<String> keySet = keys.keySet();

		try (final PreparedStatement q = c.prepareStatement("SELECT * FROM " + table + " WHERE " +
				String.join(" AND ", keySet.stream().map(k -> k + " = ?").toArray(String[]::new)) + ";")) {
			int i = 1;
			for (String k: keySet) {
				q.setObject(i++, keys.get(k));
			}

			try (final ResultSet rs = q.executeQuery()) {
				if (!rs.next()) {
					return null;
				}

				return codec.decodeValue(rs);
			}
		}
	}
}
