package hu.ssh.progressbar;


/**
 * Defines the main interfaces to work with a progressbar.
 *
 * @author KARASZI István (github@spam.raszi.hu)
 */
public interface ProgressBar<T extends ProgressBar<?>> {
	/**
	 * Starts the progress bar.
	 */
	void start();

	/**
	 * Tick one step with the progressbar.
	 */
	void tickOne();

	/**
	 * Tick the specified steps with the progressbar.
	 *
	 * @param steps
	 *            the specified steps
	 */
	void tick(long steps);

	/**
	 * Refresh the progressbar.
	 */
	void refresh();

	/**
	 * Finish the progressbar.
	 */
	void complete();

	/**
	 * Changes the total steps of the actual ProgressBar.
	 * 
	 * @param totalSteps
	 *            the new total steps
	 * @return a progress bar with the desired configuration
	 */
	T withTotalSteps(int totalSteps);
}