package org.icij.extract.extractor;

/**
 * Wraps a {@link Throwable} (typically an {@link Error}) thrown during extraction so it can be carried through APIs
 * typed on {@link Exception}, such as {@link org.icij.extract.report.Reporter#save}.
 */
public class ExtractionError extends Exception {

	public ExtractionError(final Throwable cause) {
		super(String.valueOf(cause), cause);
	}
}
