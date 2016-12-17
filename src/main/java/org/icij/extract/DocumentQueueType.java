package org.icij.extract;

import java.util.Locale;

/**
 * An enumerated list of implemented queue types.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public enum DocumentQueueType {
	NONE, ARRAY, REDIS;

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
