package org.icij.extract.interval;

import java.util.Locale;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

import java.util.concurrent.TimeUnit;

/**
 * Parses a time duration string like 1m or 500ms.
 *
 * Defaults to milliseconds for a value with no unit.
 *
 * @since 1.0.0-beta
 */
public class TimeDuration {

	private static final Pattern pattern = Pattern.compile("^(\\d+)(h|m|s|ms)?$");

	private final TimeUnit unit;
	private final long duration;

	public TimeDuration(final String duration) {
		final Matcher matcher = parse(duration);

		this.duration = Long.parseLong(matcher.group(1));
		this.unit = parseUnit(matcher);
	}

	public TimeDuration(final long duration, final TimeUnit unit) {
		this.duration = duration;
		this.unit = unit;
	}

	public static long parseTo(final String duration, final TimeUnit to) {
		final Matcher matcher = parse(duration);

		return to.convert(Long.parseLong(matcher.group(1)),
			parseUnit(matcher));
	}

	private static Matcher parse(final String duration) {
		final Matcher matcher = pattern.matcher(duration);

		if (!matcher.find()) {
			throw new IllegalArgumentException(String.format("Invalid time duration string: \"%s\".", duration));
		} else {
			return matcher;
		}
	}

	private static TimeUnit parseUnit(final Matcher matcher) {
		if (1 == matcher.groupCount()) {
			return TimeUnit.MILLISECONDS;
		} else if (matcher.group(2).equals("h")) {
			return TimeUnit.HOURS;
		} else if (matcher.group(2).equals("m")) {
			return TimeUnit.MINUTES;
		} else if (matcher.group(2).equals("s")) {
			return TimeUnit.SECONDS;
		} else {
			return TimeUnit.MILLISECONDS;
		}
	}

	public long to(final TimeUnit to) {
		return to.convert(duration, unit);
	}

	public long getDuration() {
		return duration;
	}

	public TimeUnit getUnit() {
		return unit;
	}

	public String toString() {
		return String.format("%d %s", duration, unit.name().toLowerCase(Locale.ROOT));
	}
}
