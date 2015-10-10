package hu.ssh.progressbar;

public abstract class AbstractProgressBar<T extends AbstractProgressBar<?>> implements ProgressBar<T> {
	protected final long totalSteps;

	private long actualSteps = 0;
	private long startTime = 0;

	private long lastUpdate = 0;
	private int lastUpdatePercent = 0;

	protected AbstractProgressBar(final long totalSteps) {
		this.totalSteps = totalSteps;
	}

	@Override
	public final void start() {
		refresh();
	}

	@Override
	public final void tickOne() {
		tick(1);
	}

	@Override
	public final void tick(final long steps) {
		setStartTimeIfNotStarted();

		actualSteps += steps;

		if (isRefreshNeeded()) {
			refresh();
		}
	}

	@Override
	public final void refresh() {
		setStartTimeIfNotStarted();

		final Progress progress = getProgress();

		lastUpdate = System.currentTimeMillis() / 1000;
		lastUpdatePercent = (int) (progress.getPercentage() * 100);

		updateProgressBar(progress);
	}

	@Override
	public final void complete() {
		setStartTimeIfNotStarted();

		actualSteps = totalSteps;
		refresh();
	}

	private Progress getProgress() {
		return new Progress(totalSteps, actualSteps, System.currentTimeMillis() - startTime);
	}

	private void setStartTimeIfNotStarted() {
		if (startTime == 0) {
			startTime = System.currentTimeMillis();
		}
	}

	private boolean isRefreshNeeded() {
		if (lastUpdate != System.currentTimeMillis() / 1000) {
			return true;
		}

		if (lastUpdatePercent != (int) (actualSteps * 100 / totalSteps)) {
			return true;
		}

		return false;
	}

	protected abstract void updateProgressBar(final Progress progress);

	protected abstract void finishProgressBar();
}
