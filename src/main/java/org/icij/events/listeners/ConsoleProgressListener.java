package org.icij.events.listeners;

import me.tongfei.progressbar.ProgressBar;
import org.icij.events.Listener;
import org.icij.events.Monitorable;

public class ConsoleProgressListener implements Listener {

	private final ProgressBar progress;

	public ConsoleProgressListener(final ProgressBar progress) {
		this.progress = progress;
	}

	@Override
	public void step(final Monitorable monitorable, final Object arg) {
		step(arg);
	}

	@Override
	public void step(final Object arg) {
		if (null != arg) {
			progress.setExtraMessage(arg.toString());
		}

		progress.step();
	}

	@Override
	public void steps(final int total) {
		progress.maxHint(total);
	}
}
