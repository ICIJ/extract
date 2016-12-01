package org.icij.extract.report;

import org.icij.extract.extractor.ExtractionResult;

import java.nio.file.Path;

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
	 * Check the extraction result of a given file, by path.
	 *
	 * @param file the path to check
	 * @return The extraction result or {@code null} if no result was recorded for the file.
	 */
	public ExtractionResult result(final Path file) {
		return report.get(file);
	}

	/**
	 * Save the extraction for the file at the given path.
	 *
	 * @param file path to the file
	 * @param result the extraction result
	 */
	public void save(final Path file, final ExtractionResult result) {
		report.put(file, result);
	}

	/**
	 * Check an extraction result.
	 *
	 * @param file path to the file to check
	 * @param result matched against the actual result
	 * @return {@code true} if the results match or {@code false} if there is no match or no recorded result
	 */
	public boolean check(final Path file, final ExtractionResult result) {
		final ExtractionResult status = result(file);

		return null != status && status.equals(result);
	}

	@Override
	public void close() throws Exception {
		report.close();
	}
}
