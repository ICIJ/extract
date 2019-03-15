package org.icij.extract.mysql;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

public abstract class SQLConcurrentMap<K, V> implements ConcurrentMap<K, V> {

	protected final FunctionalDataSource dataSource;
	final SQLMapCodec<K, V> codec;

	SQLConcurrentMap(final DataSource dataSource, final SQLMapCodec<K, V> codec) {
		this.dataSource = FunctionalDataSource.cast(dataSource);
		this.codec = codec;
	}

	public abstract boolean fastPut(final K key, final V value);

	public class Entry implements Map.Entry<K, V> {

		private final K key;
		private final V value;

		Entry(final K key, final V value) {
			this.key = key;
			this.value = value;
		}

		@Override
		public K getKey() {
			return key;
		}

		@Override
		public V getValue() {
			return value;
		}

		@Override
		public V setValue(final V value) {
			final V old = this.value;

			fastPut(key, value);
			return old;
		}
	}

	protected class EntrySetCodec implements SQLCodec<Map.Entry<K, V>> {

		@Override
		public Map<String, Object> encodeKey(final Object o) {
			return codec.encodeKey(o);
		}

		@Override
		public Entry decodeValue(final ResultSet rs) throws SQLException {
			return new Entry(codec.decodeKey(rs), codec.decodeValue(rs));
		}

		@Override
		public Map<String, Object> encodeValue(final Object o) {
			return codec.encodeValue(o);
		}
	}

	protected class KeySetCodec implements SQLCodec<K> {

		@Override
		public Map<String, Object> encodeKey(final Object o) {
			return codec.encodeKey(o);
		}

		@Override
		public K decodeValue(final ResultSet rs) throws SQLException {
			return codec.decodeKey(rs);
		}

		@Override
		public Map<String, Object> encodeValue(final Object o) {
			return codec.encodeValue(o);
		}
	}

	protected class ValuesCodec implements SQLCodec<V> {

		@Override
		public Map<String, Object> encodeKey(final Object o) {
			return codec.encodeKey(o);
		}

		@Override
		public V decodeValue(final ResultSet rs) throws SQLException {
			return codec.decodeValue(rs);
		}

		@Override
		public Map<String, Object> encodeValue(final Object o) {
			return codec.encodeValue(o);
		}
	}
}
