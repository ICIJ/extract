package org.icij.extract.core;

import java.util.Locale;

/**
 * List of report backend types.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public enum ReportType {
	NONE, REDIS;

	public String toString() {
		return name().toLowerCase(Locale.ROOT);
	}

	public static ReportType parse(final String reportType) {
		if (null == reportType) {
			return NONE;
		}

		try {
			return valueOf(reportType.toUpperCase(Locale.ROOT));
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException(String.format("\"%s\" is not a valid report type.", reportType));
		}
	}
}
