package org.icij.extract.core;

import java.util.Locale;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public enum QueueType {
	NONE, ARRAY, REDIS;

	public String toString() {
		return name().toLowerCase(Locale.ROOT);
	}

	public static QueueType parse(final String queueType) {
		if (null == queueType) {
			return NONE;
		}

		try {
			return valueOf(queueType.toUpperCase(Locale.ROOT));
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException(String.format("\"%s\" is not a valid queue type.", queueType));
		}
	}
}
