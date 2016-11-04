package org.icij.extract;

import java.util.Locale;

/**
 * An enumerated list of implemented output types.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public enum OutputType {
	FILE, STDOUT, SOLR;

	/**
	 * Print a friendly name for the output type.
	 *
	 * @return An all-lowercase name.
	 */
	public String toString() {
		return name().toLowerCase(Locale.ROOT);
	}

	/**
	 * Create an instance from a string.
	 *
	 * @param outputType the string to be parsed into an output type
	 * @return An instance of the output type.
	 */
	public static OutputType parse(final String outputType) {
		try {
			return valueOf(outputType.toUpperCase(Locale.ROOT));
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException(String.format("\"%s\" is not a valid output type.", outputType));
		}
	}
}
