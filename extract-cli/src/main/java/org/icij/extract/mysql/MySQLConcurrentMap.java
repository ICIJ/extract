package org.icij.extract.mysql;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class MySQLConcurrentMap<K, V> extends SQLConcurrentMap<K, V> {

	private final String table;

	public MySQLConcurrentMap(final DataSource dataSource, final SQLMapCodec<K, V> codec, final String table) {
		super(dataSource, codec);
		this.table = table;
	}

	private void executeInsert(final Connection c, final K key, final V value) throws SQLException {
		final Map<String, Object> values = codec.encodeValue(value);
		values.putAll(codec.encodeKey(key));

		try (final PreparedStatement q = c.prepareStatement("INSERT " + table + " SET " +
				String.join(",", values.keySet().stream().map(k -> k + " = ?").toArray(String[]::new)) + ";")) {

			int i = 1;
			for (String k : values.keySet()) {
				q.setObject(i++, values.get(k));
			}

			q.executeUpdate();
		}
	}

	private int executeInsertOrUpdate(final Connection c, final K key, final V value) throws SQLException {
		final Map<String, Object> values = codec.encodeValue(value);
		values.putAll(codec.encodeKey(key));

		final Set<String> keys = values.keySet();

		final String sql = "INSERT INTO " + table + " (" +
				String.join(", ", keys.toArray(new String[keys.size()])) +
				") VALUES(" +
				String.join(", ", keys.stream().map(k -> "?").toArray(String[]::new)) +
				") ON DUPLICATE KEY UPDATE " +
				String.join(", ", keys.stream().map(k -> k + " = VALUES(" + k + ")").toArray(String[]::new)) + ";";

		try (final PreparedStatement q = c.prepareStatement(sql)) {
			int i = 1;

			for (String k : keys) {
				q.setObject(i++, values.get(k));
			}

			return q.executeUpdate();
		}
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

	private V executeSelectForUpdate(final Connection c, final Object key) throws SQLException {
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

	private V executeSelect(final Connection c, final Object key) throws SQLException {
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

	@Override
	public boolean replace(final K key, final V oldValue, final V newValue) {
		return dataSource.withConnectionUnchecked(c -> {
			final int result;
			c.setAutoCommit(false);

			try {
				if (!oldValue.equals(executeSelectForUpdate(c, key))) {
					c.rollback();
					return false;
				}

				result = executeUpdate(c, key, newValue);
			} catch (final SQLException e) {
				c.rollback();
				throw e;
			}

			c.commit();
			return result > 0;
		});
	}

	@Override
	public V replace(final K key, final V value) {
		return dataSource.withConnectionUnchecked(c -> {
			final V oldValue;
			c.setAutoCommit(false);

			try {
				oldValue = executeSelectForUpdate(c, key);
				executeUpdate(c, key, value);
			} catch (SQLException e) {
				c.rollback();
				throw e;
			}

			c.commit();
			return oldValue;
		});
	}

	@Override
	public void clear() {
		dataSource.withStatementUnchecked("DELETE FROM " + table + ";",
				(CheckedConsumer<PreparedStatement>) PreparedStatement::executeUpdate);
	}

	@Override
	public boolean remove(final Object key, final Object value) {
		return dataSource.withConnectionUnchecked(c -> {
			final int result;
			c.setAutoCommit(false);

			try {
				if (!value.equals(executeSelectForUpdate(c, key))) {
					c.rollback();
					return false;
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
				for (String k : keySet) {
					q.setObject(i++, values.get(k));
				}

				result = q.executeUpdate();
			} catch (SQLException e) {
				c.rollback();
				throw e;
			}

			c.commit();
			return result > 0;
		});
	}

	@Override
	public V remove(final Object key) {
		return dataSource.withConnectionUnchecked(c -> {
			final V oldValue;
			c.setAutoCommit(false);

			try {
				oldValue = executeSelectForUpdate(c, key);
			} catch (SQLException e) {
				c.rollback();
				throw e;
			}

			final Map<String, Object> keys = codec.encodeKey(key);
			final Set<String> keySet = keys.keySet();

			try (final PreparedStatement q = c.prepareStatement("DELETE FROM " + table + " WHERE " +
					String.join(" AND ", keySet.stream().map(k -> k + " = ?").toArray(String[]::new)) + ";")) {
				int i = 1;
				for (String k : keySet) {
					q.setObject(i++, keys.get(k));
				}

				q.executeUpdate();
			} catch (SQLException e) {
				c.rollback();
				throw e;
			}

			c.commit();
			return oldValue;
		});
	}

	@Override
	public V put(final K key, final V value) {
		return dataSource.withConnectionUnchecked(c -> {
			final V oldValue;
			c.setAutoCommit(false);

			try {
				oldValue = executeSelectForUpdate(c, key);
			} catch (SQLException e) {
				c.rollback();
				throw e;
			}

			try {
				if (null == oldValue) {
					executeUpdate(c, key, value);
				} else {

					// There's a race condition here, like with #putIfAbsent. See below.
					// TODO: use a lock.
					executeInsert(c, key, value);
				}
			} catch (SQLException e) {
				c.rollback();
				throw e;
			}

			c.commit();
			return oldValue;
		});
	}

	@Override
	public V putIfAbsent(final K key, final V value) {
		return dataSource.withConnectionUnchecked(c -> {
			final V oldValue = executeSelect(c, key);

			if (null != oldValue) {
				return oldValue;
			}

			// There's a race condition here. An exception might be thrown if a record with the same keys is inserted
			// between the call to #get(...) and this point.
			// TODO: use a lock.
			executeInsert(c, key, value);
			return null;
		});
	}

	@Override
	public boolean fastPut(final K key, final V value) {
		return dataSource.withConnectionUnchecked(c -> executeInsertOrUpdate(c, key, value) > 0);
	}

	@Override
	public int size() {
		return dataSource.withStatementUnchecked("SELECT COUNT(*) FROM " + table + ";", q -> {
			try (final ResultSet rs = q.executeQuery()) {
				rs.next();
				return rs.getInt(1);
			}
		});
	}

	@Override
	public void putAll(final Map<? extends K, ? extends V> m) {
		dataSource.withConnectionUnchecked(c -> {
			for (Map.Entry<? extends K, ? extends V> e : m.entrySet()) {
				executeInsertOrUpdate(c, e.getKey(), e.getValue());
			}
		});
	}

	@Override
	public boolean containsKey(final Object key) {
		final Map<String, Object> keys = codec.encodeKey(key);
		final Set<String> keySet = keys.keySet();

		return dataSource.withStatementUnchecked("SELECT EXISTS(SELECT * FROM " + table + " WHERE " +
				String.join(" AND ", keySet.stream().map(k -> k + " = ?").toArray(String[]::new)) + ";", q -> {
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
	public boolean containsValue(final Object value) {
		final Map<String, Object> values = codec.encodeValue(value);
		final Set<String> keySet = values.keySet();

		return dataSource.withStatementUnchecked("SELECT EXISTS(SELECT * FROM " + table + " WHERE " +
				String.join(" AND ", keySet.stream().map((k)-> k + " = ?").toArray(String[]::new)) + ");", q -> {
			int i = 1;
			for (String k: keySet) {
				q.setObject(i++, values.get(k));
			}

			try (final ResultSet rs = q.executeQuery()) {
				rs.next();
				return rs.getBoolean(1);
			}
		});
	}

	@Override
	public boolean isEmpty() {
		return dataSource.withStatementUnchecked("SELECT EXISTS(SELECT * FROM " + table + ");", q -> {
			final ResultSet rs = q.executeQuery();

			rs.next();
			return rs.getBoolean(1);
		});
	}

	@Override
	public V get(final Object key) {
		return dataSource.withConnectionUnchecked(c -> {
			return executeSelect(c, key);
		});
	}

	@Override
	public Set<Map.Entry<K, V>> entrySet() {
		return new MySQLSet<>(dataSource, new EntrySetCodec(), table);
	}

	@Override
	public Set<K> keySet() {
		return new MySQLSet<>(dataSource, new KeySetCodec(), table);
	}

	@Override
	public Collection<V> values() {
		return new MySQLSet<>(dataSource, new ValuesCodec(), table);
	}
}
