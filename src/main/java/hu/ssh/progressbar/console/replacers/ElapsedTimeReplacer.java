package hu.ssh.progressbar.console.replacers;

import hu.ssh.progressbar.Progress;
import hu.ssh.progressbar.helpers.HumanTimeFormatter;

public class ElapsedTimeReplacer implements Replacer {
	private static final String IDENTIFIER = ":elapsed";

	@Override
	public final String getReplaceIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public final String getReplacementForProgress(final Progress progress) {
		return HumanTimeFormatter.formatTime(progress.getElapsedTime());
	}
}
