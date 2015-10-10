package org.icij.extract.core;

import java.util.Map;

import java.util.logging.Logger;

import java.nio.file.Path;

/**
 * Logs the status of a file to the given {@link Map}.
 *
 * @since 1.0.0-beta
 */
public class Reporter {
	public static final int SUCCEEDED = 0;
	public static final int NOT_FOUND = 1;
	public static final int NOT_READ = 2;
	public static final int NOT_DECRYPTED = 3;
	public static final int NOT_PARSED = 4;
	public static final int NOT_CLEAR = 9;
	public static final int NOT_SAVED = 10;

	protected final Logger logger;
	protected final Map<String, Integer> report;

	public Reporter(final Logger logger, final Map<String, Integer> report) {
		this.logger = logger;
		this.report = report;
	}

	public Integer status(final Path file) {
		return (Integer) report.get(file.toString());
	}

	public boolean succeeded(final Path file) {
		final Integer status = status(file);

		if (null != status) {
			return status == SUCCEEDED;
		}

		return false;
	}

	public void save(final Path file, final int status) {
		report.put(file.toString(), status);
	}
}
