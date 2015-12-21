package org.icij.extract.core;

import java.io.Closeable;
import java.io.IOException;

import java.nio.file.Path;

/**
 * Logs the extraction result of a file to the given {@link Report}.
 *
 * @since 1.0.0-beta
 */
public class Reporter implements Closeable {

	protected final Report report;

	public Reporter(final Report report) {
		this.report = report;
	}

	public ReportResult result(final Path file) {
		return report.get(file);
	}

	public void save(final Path file, final ReportResult result) {
		report.put(file, result);
	}

	public boolean check(final Path file, final ReportResult result) {
		final ReportResult status = result(file);

		return null != status && status.equals(result);
	}

	@Override
	public void close() throws IOException {
		report.close();
	}
}
