package org.icij.extract.report;

import java.util.Locale;

/**
 * An enumerated list of implemented report map types.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public enum ReportMapType {
	REDIS, MYSQL, HASH;

	/**
	 * Return the name of the report type.
	 *
	 * @return The name of the map type.
	 */
	public String toString() {
		return name().toLowerCase(Locale.ROOT);
	}

	/**
	 * Parse the given string representation of the type into an instance.
	 *
	 * @param mapType the type of report map as a string value
	 * @return The type of report map as a {@link ReportMapType} instance.
	 */
	public static ReportMapType parse(final String mapType) {
		try {
			return valueOf(mapType.toUpperCase(Locale.ROOT));
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException(String.format("\"%s\" is not a valid report type.", mapType));
		}
	}
}
