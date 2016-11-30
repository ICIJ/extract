package org.icij.extract;

import java.util.Locale;

/**
 * An enumerated list of implemented report types.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public enum ReportType {
	REDIS;

	/**
	 * Return the name of the report type.
	 *
	 * @return The name of the queue type.
	 */
	public String toString() {
		return name().toLowerCase(Locale.ROOT);
	}

	/**
	 * Parse the given string representation of the type into an instance.
	 *
	 * @param reportType the type of report as a string value
	 * @return The type of report as a {@link ReportType} instance.
	 */
	public static ReportType parse(final String reportType) {
		try {
			return valueOf(reportType.toUpperCase(Locale.ROOT));
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException(String.format("\"%s\" is not a valid report type.", reportType));
		}
	}
}
