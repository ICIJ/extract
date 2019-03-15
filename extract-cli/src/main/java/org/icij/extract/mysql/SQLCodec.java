package org.icij.extract.mysql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public interface SQLCodec<T> {

	/**
	 * Encode the "key", which is map of column names to values used to uniquely look up an object.
	 *
	 * @param o the object to encode
	 * @return a column name and key value map
	 */
	Map<String, Object> encodeKey(final Object o);

	/**
	 * Decode a result set into an object instance.
	 *
	 * @return a new instance of the codec type
	 */
	T decodeValue(final ResultSet rs) throws SQLException;

	/**
	 * Encode an object instance into a map of column names and values.
	 *
	 * @param o the instance to encode
	 * @return a column name and value map
	 */
	Map<String, Object> encodeValue(final Object o);
}
