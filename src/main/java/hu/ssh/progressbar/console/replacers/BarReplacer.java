package hu.ssh.progressbar.console.replacers;

import hu.ssh.progressbar.Progress;

import com.google.common.base.Strings;

public class BarReplacer implements Replacer {
	private static final String IDENTIFIER = ":bar";
	private static final String COMPLETED_CHAR = "=";
	private static final char REMAINING_CHAR = '-';

	private final int totalWidth;

	public BarReplacer(final int totalWidth) {
		this.totalWidth = totalWidth;
	}

	@Override
	public final String getReplaceIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public final String getReplacementForProgress(final Progress progress) {
		final int actualWidth = (int) (progress.getPercentage() * totalWidth);

		final String bar = Strings.repeat(COMPLETED_CHAR, actualWidth);
		return Strings.padEnd(bar, totalWidth, REMAINING_CHAR);
	}
}
