package hu.ssh.progressbar;

public class Progress {
	private static final int MINIMAL_ELAPSED = 100;

	private final long totalSteps;
	private final long actualSteps;
	private final long elapsedTime;

	public Progress(final long totalSteps, final long actualSteps, final long elapsedTime) {
		this.totalSteps = totalSteps;
		this.actualSteps = actualSteps;
		this.elapsedTime = elapsedTime;
	}

	public final long getTotalSteps() {
		return totalSteps;
	}

	public final long getActualSteps() {
		return actualSteps;
	}

	public final long getElapsedTime() {
		return elapsedTime;
	}

	public final float getPercentage() {
		return (float) actualSteps / totalSteps;
	}

	public final long getRemainingTime() {
		return getTotalTime() - elapsedTime;
	}

	public final long getTotalTime() {
		return (long) (elapsedTime / getPercentage());
	}

	public final boolean isRemainingTimeReliable() {
		return elapsedTime > MINIMAL_ELAPSED;
	}
}