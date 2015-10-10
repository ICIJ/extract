package hu.ssh.progressbar.console.replacers;

import hu.ssh.progressbar.Progress;

public interface Replacer {
	String getReplaceIdentifier();

	String getReplacementForProgress(Progress progress);
}
