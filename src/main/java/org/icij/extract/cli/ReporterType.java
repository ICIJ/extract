package org.icij.extract.cli;

import org.icij.extract.core.*;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public enum ReporterType {
	NONE, REDIS;

	public String toString() {
		return name().toLowerCase();
	}

	public static final ReporterType parse(String reporterType) {
		if (null == reporterType) {
			return NONE;
		}

		try {
			return fromString(reporterType);
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException(String.format("\"%s\" is not a valid reporter type.", reporterType));
		}
	}

	public static final ReporterType fromString(String reporterType) {
		return valueOf(reporterType.toUpperCase());
	}
}
