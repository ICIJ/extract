package org.icij.extract.extractor;

/**
 * Classification helpers for throwables raised during extraction.
 *
 * <p>A <em>fatal</em> error leaves the JVM in an uncertain state (notably {@link OutOfMemoryError}); the process
 * should record the failure best-effort and then exit so it can be restarted clean. A <em>recoverable</em> error
 * (such as a parser's {@link StackOverflowError}) affects only the current document and processing can continue.
 */
public final class ExtractionErrors {

	private ExtractionErrors() {}

	/**
	 * @param t the throwable to classify
	 * @return {@code true} for {@link OutOfMemoryError} and any {@link VirtualMachineError} other than
	 *         {@link StackOverflowError}; {@code false} otherwise.
	 */
	public static boolean isFatal(final Throwable t) {
		return t instanceof OutOfMemoryError
				|| (t instanceof VirtualMachineError && !(t instanceof StackOverflowError));
	}

	/**
	 * Adapt a throwable to {@link Exception} so it can be passed to APIs typed on {@code Exception}.
	 *
	 * @param t the throwable to adapt
	 * @return {@code t} itself when it is already an {@link Exception}, otherwise an {@link ExtractionError}
	 *         wrapping it.
	 */
	public static Exception asException(final Throwable t) {
		return (t instanceof Exception) ? (Exception) t : new ExtractionError(t);
	}
}
