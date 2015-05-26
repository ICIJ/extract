package org.icij.extract.core;

import java.util.logging.Logger;

import java.nio.file.Path;

import org.redisson.Redisson;
import org.redisson.core.RMap;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public class RedisReporter extends Reporter {
	public static final int SUCCEEDED = 0;
	public static final int NOT_FOUND = 1;
	public static final int NOT_READ = 2;
	public static final int NOT_DECRYPTED = 3;
	public static final int NOT_PARSED = 4;
	public static final int NOT_SAVED = 10;

	public static RMap<String, Integer> getReport(String namespace, Redisson redisson) {
		return redisson.getMap(namespace + ":report");
	}

	public RedisReporter(Logger logger, RMap<String, Integer> report) {
		super(logger, report);
	}

	public void save(Path file, int status) {
		((RMap) report).fastPut(file.toString(), status);
	}
}
