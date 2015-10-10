package hu.ssh.progressbar.console.replacers;

import hu.ssh.progressbar.Progress;

public class PercentageReplacer implements Replacer {
	private static final String IDENTIFIER = ":percent";

	@Override
	public final String getReplaceIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public final String getReplacementForProgress(final Progress progress) {
		return String.format("%6.2f", progress.getPercentage() * 100);
	}
}
