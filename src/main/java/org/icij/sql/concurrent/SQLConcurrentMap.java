package org.icij.sql.concurrent;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class SQLConcurrentMap<K, V> extends AbstractConcurrentMap<K, V> {

	private final DataSource ds;
	private final SQLConcurrentMapAdapter<K, V> adapter;

	public SQLConcurrentMap(final DataSource ds, final SQLConcurrentMapAdapter<K, V> adapter) {
		this.ds = ds;
		this.adapter = adapter;
	}

	@Override
	public Set<K> keySet() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<Map.Entry<K,V>> entrySet() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Collection<V> values() {
		throw new UnsupportedOperationException();
	}

	@Override
	public int size() {
		try (final Connection c = ds.getConnection()) {
			return adapter.size(c);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public boolean isEmpty() {
		try (final Connection c = ds.getConnection()) {
			return adapter.isEmpty(c);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public boolean containsKey(final Object key) {
		try (final Connection c = ds.getConnection()) {
			return adapter.containsKey(c, key);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public V get(final Object key) {
		try (final Connection c = ds.getConnection()) {
			return adapter.get(c, key);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		try (final Connection c = ds.getConnection()) {
			adapter.putAll(c, m);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public boolean containsValue(final Object o) {
		try (final Connection c = ds.getConnection()) {
			return adapter.containsValue(c, o);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public V put(final K key, final V value) {
		try (final Connection c = ds.getConnection()) {
			return adapter.put(c, key, value);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public V putIfAbsent(final K key, final V value) {
		try (final Connection c = ds.getConnection()) {
			return adapter.putIfAbsent(c, key, value);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public boolean fastPut(final K key, final V value) {
		try (final Connection c = ds.getConnection()) {
			return adapter.fastPut(c, key, value);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public boolean remove(final Object key, final Object value) {
		try (final Connection c = ds.getConnection()) {
			return adapter.remove(c, key, value) > 0;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public V remove(final Object key) {
		try (final Connection c = ds.getConnection()) {
			return adapter.remove(c, key);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public V replace(final K key, final V value) {
		try (final Connection c = ds.getConnection()) {
			return adapter.replace(c, key, value);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public boolean replace(final K key, final V oldValue, final V newValue) {
		try (final Connection c = ds.getConnection()) {
			return adapter.replace(c, key, oldValue, newValue) > 0;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void clear() {
		try (final Connection c = ds.getConnection()) {
			adapter.clear(c);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
}
