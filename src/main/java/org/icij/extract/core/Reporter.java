package org.icij.extract.core;

import java.util.Map;

import java.util.logging.Logger;

import java.nio.file.Path;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public abstract class Reporter {
	public static final int SUCCEEDED = 0;
	public static final int NOT_FOUND = 1;
	public static final int NOT_READ = 2;
	public static final int NOT_DECRYPTED = 3;
	public static final int NOT_PARSED = 4;
	public static final int NOT_SAVED = 10;

	protected final Logger logger;
	protected final Map<String, Integer> report;

	public Reporter(Logger logger, Map<String, Integer> report) {
		this.logger = logger;
		this.report = report;
	}

	public boolean succeeded(Path file) {
		return false;//report.get(file.toString()) == SUCCEEDED;
	}

	public void save(Path file, int status) {
		report.put(file.toString(), status);
	}
}
