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

	public static RMap<String, Integer> getReport(String namespace, Redisson redisson) {
		if (null == namespace) {
			namespace = "extract";
		}

		return redisson.getMap(namespace + ":report");
	}

	public RedisReporter(Logger logger, RMap<String, Integer> report) {
		super(logger, report);
	}

	public void save(Path file, int status) {
		((RMap) report).fastPut(file.toString(), status);
	}
}
