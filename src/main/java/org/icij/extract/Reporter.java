package org.icij.extract;

import java.util.logging.Logger;

import java.nio.file.Path;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public class Reporter {
	public static final int SUCCEEDED = 0;
	public static final int NOT_FOUND = 1;
	public static final int NOT_READ = 2;
	public static final int NOT_DECRYPTED = 3;
	public static final int NOT_PARSED = 4;
	public static final int NOT_SAVED = 10;

	private final Logger logger;

	public Reporter(Logger logger) {
		this.logger = logger;
	}

	public boolean succeeded(Path file) {
		return false;
	}

	public void save(Path file, int status) {
		
	}
}
