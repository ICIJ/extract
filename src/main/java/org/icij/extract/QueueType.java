package org.icij.extract;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public enum QueueType {
	MEMORY, REDIS;

	public String toString() {
		return name().toLowerCase();
	}

	public static final QueueType parse(String queueType) {
		try {
			return QueueType.fromString(queueType);
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException(String.format("\"%s\" is not a valid queue type.", queueType));
		}
	}

	public static final QueueType fromString(String queueType) {
		return QueueType.valueOf(queueType.toUpperCase());
	}
}
