package org.icij.extract;

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
		return name().toLowerCase();
	}

	public static final OutputType fromString(String outputType) {
		return OutputType.valueOf(outputType.toUpperCase());
	}
};
