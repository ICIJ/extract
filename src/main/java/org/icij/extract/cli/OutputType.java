package org.icij.extract.cli;

import java.util.Locale;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public enum OutputType {
	FILE, STDOUT, SOLR;

	public String toString() {
		return name().toLowerCase(Locale.ROOT);
	}

	public static OutputType fromString(String outputType) {
		return valueOf(outputType.toUpperCase(Locale.ROOT));
	}
}
