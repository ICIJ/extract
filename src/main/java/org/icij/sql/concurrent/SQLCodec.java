package org.icij.sql.concurrent;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public interface SQLCodec<T> {

	String getKeyName();

	String encodeKey(final Object o);

	/**
	 * Decode a result set into an instance.
	 *
	 * @return a new instance of the codec type
	 */
	T decodeValue(final ResultSet rs) throws SQLException;

	/**
	 * Encode an instance into a map of keys and values.
	 *
	 * @param o the instance to encode
	 * @return a key-value map
	 */
	Map<String, Object> encodeValue(final Object o);
}
