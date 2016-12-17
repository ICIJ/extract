package org.icij.extract.report;

import org.icij.extract.document.Document;
import org.icij.extract.extractor.ExtractionStatus;

/**
 * Records the extraction result of a file to the given {@link Report}.
 *
 * @since 1.0.0-beta
 */
public class Reporter implements AutoCloseable {

	/**
	 * The report to save results to or check.
	 */
	protected final Report report;

	/**
	 * Create a new reporter that will record results to the given {@link Report}.
	 *
	 * @param report the report map
	 */
	public Reporter(final Report report) {
		this.report = report;
	}

	/**
	 * Check the extraction result of a given document.
	 *
	 * @param document the document to check
	 * @return The extraction result or {@code null} if no result was recorded for the file.
	 */
	public ExtractionStatus result(final Document document) {
		return report.get(document);
	}

	/**
	 * Save the extraction for the given document.
	 *
	 * @param document the document
	 * @param result the extraction result
	 */
	public void save(final Document document, final ExtractionStatus result) {
		report.put(document, result);
	}

	/**
	 * Check an extraction result.
	 *
	 * @param document the document to check
	 * @param result matched against the actual result
	 * @return {@code true} if the results match or {@code false} if there is no match or no recorded result.
	 */
	public boolean check(final Document document, final ExtractionStatus result) {
		final ExtractionStatus status = result(document);

		return null != status && status.equals(result);
	}

	/**
	 * Check whether a path should be skipped.
	 *
	 * @param document the document to check
	 * @return {@code true} if the document should be skipped.
	 */
	public boolean skip(final Document document) {
		return check(document, ExtractionStatus.SUCCEEDED);
	}

	@Override
	public void close() throws Exception {
		report.close();
	}
}
