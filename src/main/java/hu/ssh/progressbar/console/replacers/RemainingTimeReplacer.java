package hu.ssh.progressbar.console.replacers;

import hu.ssh.progressbar.Progress;
import hu.ssh.progressbar.helpers.HumanTimeFormatter;

public class RemainingTimeReplacer implements Replacer {
	private static final String IDENTIFIER = ":eta";

	@Override
	public final String getReplaceIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public final String getReplacementForProgress(final Progress progress) {
		if (!progress.isRemainingTimeReliable()) {
			return "?";
		}

		return HumanTimeFormatter.formatTime(progress.getRemainingTime());
	}
}
