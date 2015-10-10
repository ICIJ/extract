package hu.ssh.progressbar.helpers;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

public class HumanTimeFormatter extends Helper {
	private static final List<DisplayUnit> DISPLAY_UNITS = ImmutableList.of(
			DisplayUnit.of(TimeUnit.DAYS, "d"),
			DisplayUnit.of(TimeUnit.HOURS, "h"),
			DisplayUnit.of(TimeUnit.MINUTES, "m"),
			DisplayUnit.of(TimeUnit.SECONDS, "s"),
			DisplayUnit.of(TimeUnit.MILLISECONDS, "ms")
			);

	public static String formatTime(final long milliseconds) {
		long diff = milliseconds;

		final StringBuilder sb = new StringBuilder();

		final Iterator<DisplayUnit> iterator = DISPLAY_UNITS.iterator();

		while (iterator.hasNext()) {
			final DisplayUnit displayUnit = iterator.next();

			final long value = displayUnit.getFromMilliseconds(diff);

			if (value != 0 || (!iterator.hasNext() && sb.length() == 0)) {
				sb.append(displayUnit.getWithSuffix(value));
				diff -= displayUnit.getInMilliseconds(value);
			}
		}

		return sb.toString();
	}

	private static final class DisplayUnit {
		private final TimeUnit timeUnit;
		private final String suffix;

		private DisplayUnit(final TimeUnit timeUnit, final String suffix) {
			this.timeUnit = Preconditions.checkNotNull(timeUnit);
			this.suffix = Preconditions.checkNotNull(suffix);
		}

		public static DisplayUnit of(final TimeUnit timeUnit, final String suffix) {
			return new DisplayUnit(timeUnit, suffix);
		}

		public long getFromMilliseconds(final long milliseconds) {
			return timeUnit.convert(milliseconds, TimeUnit.MILLISECONDS);
		}

		public long getInMilliseconds(final long value) {
			return TimeUnit.MILLISECONDS.convert(value, timeUnit);
		}

		public String getWithSuffix(final long value) {
			return String.format("%d%s", value, suffix);
		}
	}
}
