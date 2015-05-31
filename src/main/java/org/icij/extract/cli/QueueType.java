package org.icij.extract.cli;

import org.icij.extract.core.*;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public enum QueueType {
	NONE, REDIS;

	public String toString() {
		return name().toLowerCase(Locale.ROOT);
	}

	public static final QueueType parse(String queueType) {
		if (null == queueType) {
			return NONE;
		}

		try {
			return fromString(queueType);
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException(String.format("\"%s\" is not a valid queue type.", queueType));
		}
	}

	public static final QueueType fromString(String queueType) {
		return valueOf(queueType.toUpperCase(Locale.ROOT));
	}
}
