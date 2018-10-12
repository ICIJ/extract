package org.icij.extract.queue;

import java.util.Locale;

/**
 * An enumerated list of implemented queue types.
 *
 *
 */
public enum DocumentQueueType {
	ARRAY, REDIS, MYSQL;

	/**
	 * Return the name of the queue type.
	 *
	 * @return The name of the queue type.
	 */
	public String toString() {
		return name().toLowerCase(Locale.ROOT);
	}

	/**
	 * Parse the given string representation of the type into an instance.
	 *
	 * @param queueType the type of queue as a string value
	 * @return The type of queue as a {@link DocumentQueueType} instance.
	 */
	public static DocumentQueueType parse(final String queueType) {
		try {
			return valueOf(queueType.toUpperCase(Locale.ROOT));
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException(String.format("\"%s\" is not a valid queue type.", queueType));
		}
	}
}
