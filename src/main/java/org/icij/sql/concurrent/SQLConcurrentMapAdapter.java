package org.icij.sql.concurrent;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

public interface SQLConcurrentMapAdapter<K, V> {

	int replace(final Connection c, final K key, final V oldValue, final V newValue) throws SQLException;

	V replace(final Connection c, final K key, final V value) throws SQLException;

	int clear(final Connection c) throws SQLException;

	int remove(final Connection c, final Object key, final Object value) throws SQLException;

	V remove(final Connection c, final Object key) throws SQLException;

	V put(final Connection c, final K key, final V value) throws SQLException;

	V putIfAbsent(final Connection c, final K key, final V value) throws SQLException;

	void putAll(final Connection c, final Map<? extends K, ? extends V> m) throws SQLException;

	boolean containsValue(final Connection c, final Object o) throws SQLException;

	int size(final Connection c) throws SQLException;

	boolean isEmpty(final Connection c) throws SQLException;

	V get(final Connection c, final Object key) throws SQLException;

	boolean containsKey(final Connection c, final Object key) throws SQLException;
}
