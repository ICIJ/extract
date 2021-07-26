package org.icij.extract.mysql;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface SQLMapCodec<K, V> extends SQLCodec<V> {

	/**
	 * Decode a result set into a key instance.
	 *
	 * @return a new instance of the codec key type
	 */
	K decodeKey(final ResultSet rs) throws SQLException;
}
