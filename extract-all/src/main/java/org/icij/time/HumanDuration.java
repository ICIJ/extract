package org.icij.time;

import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 * Parses a "human" time duration string like {@literal 1m} or {@literal 500ms} to a {@link java.time.Duration}.
 *
 * Defaults to milliseconds for a value with no unit.
 */
public class HumanDuration {

	private static final Pattern pattern = Pattern.compile("^(\\d+)(d|h|m|s|ms)?$", Pattern.CASE_INSENSITIVE);

	/**
	 * Creates a new {@link Duration} by parsing the given string.
	 *
	 * @param duration the duration - for example {@literal 500ms} or {@literal 1h}
	 */
	public static Duration parse(final String duration) {
		final Matcher matcher = pattern.matcher(duration);
		final long value;

		if (!matcher.find()) {
			throw new DateTimeParseException("Invalid time duration string.", duration, 0);
		}

		value = Long.parseLong(matcher.group(1));

		if (null == matcher.group(2)) {
			return Duration.ofMillis(value);
		}

		switch (matcher.group(2).toLowerCase()) {
			case "d":
				return Duration.ofDays(value);

			case "h":
				return Duration.ofHours(value);

			case "m":
				return Duration.ofMinutes(value);

			case "s":
				return Duration.ofSeconds(value);

			default:
				return Duration.ofMillis(value);
		}
	}

	/**
	 * Convert the duration to a string of the same format that is accepted by {@link #parse(String)}.
	 *
	 * @return A formatted string representation of the duration.
	 */
	public static String format(final Duration duration) {
		long value = duration.toMillis();

		if (value >= 1000) {
			value = duration.getSeconds();
		} else {
			return String.format("%sms", value);
		}

		if (value >= 60) {
			value = duration.toMinutes();
		} else {
			return String.format("%ss", value);
		}

		if (value >= 60) {
			value = duration.toHours();
		} else {
			return String.format("%sm", value);
		}

		if (value >= 24) {
			value = duration.toDays();
		} else {
			return String.format("%sh", value);
		}

		return String.format("%sd", value);
	}
}
