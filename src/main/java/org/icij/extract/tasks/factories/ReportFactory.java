package org.icij.extract.tasks.factories;

import org.icij.extract.core.Report;
import org.icij.extract.ReportType;
import org.icij.extract.redis.RedisReport;
import org.icij.extract.redis.ConnectionManagerFactory;

import org.icij.task.Options;

/**
 * Factory methods for creating {@link Report} objects.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public class ReportFactory {

	private ReportType type = null;
	private Options<String> options = null;

	public ReportFactory withOptions(final Options<String> options) {
		type = options.get("report-type").asEnum(ReportType::parse).orElse(null);
		this.options = options;
		return this;
	}

	/**
	 * Create a new report from the given arguments.
	 *
	 * @return a new report or {@code null} if no type is specified
	 */
	public Report create() {
		if (null == type) {
			return null;
		}

		return createShared();
	}

	/**
	 * Create a new Redis-backed report from commandline parameters.
	 *
	 * @return a new Redis-backed report
	 * @throws IllegalArgumentException if the given options do not contain a valid shared report type
	 */
	public Report createShared() throws IllegalArgumentException {
		if (ReportType.REDIS != type) {
			throw new IllegalArgumentException(String.format("\"%s\" is not a valid shared report type.", type));
		}

		return new RedisReport(options);
	}
}
