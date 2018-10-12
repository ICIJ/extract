package org.icij.extract;

import java.util.Locale;

/**
 * An enumerated list of implemented index types.
 *
 *
 */
public enum IndexType {
	SOLR;

	/**
	 * Print a friendly name for the index type.
	 *
	 * @return An all-lowercase name.
	 */
	public String toString() {
		return name().toLowerCase(Locale.ROOT);
	}

	/**
	 * Create an instance from a string.
	 *
	 * @param indexType the string to be parsed into an index type
	 * @return An instance of the index type.
	 */
	public static IndexType parse(final String indexType) {
		try {
			return valueOf(indexType.toUpperCase(Locale.ROOT));
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException(String.format("\"%s\" is not a valid index type.", indexType));
		}
	}
}
