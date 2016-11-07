package org.icij.extract.tasks.factories;

import org.icij.extract.core.Report;
import org.icij.extract.ReportType;
import org.icij.extract.redis.RedisReport;

import org.icij.task.DefaultOption;

import java.util.Optional;

/**
 * Factory methods for creating {@link Report} objects.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public class ReportFactory {

	/**
	 * Create a new report from commandline parameters.
	 *
	 * @param options the options for creating the report
	 * @return a new report or {@code null} if no type is specified
	 */
	public static Report createReport(final DefaultOption.Set options) {
		final Optional<ReportType> reportType = options.get("report-type").asEnum(ReportType::parse);

		if (!reportType.isPresent()) {
			return null;
		}

		return createSharedReport(options);
	}

	/**
	 * Create a new Redis-backed report from commandline parameters.
	 *
	 * @param options the options for creating the report
	 * @return a new Redis-backed report
	 * @throws IllegalArgumentException if the given options do not contain a valid shared report type
	 */
	public static Report createSharedReport(final DefaultOption.Set options) throws IllegalArgumentException {
		final ReportType reportType = options.get("report-type").asEnum(ReportType::parse).orElse(ReportType.REDIS);
		final Optional<String> name = options.get("report-name").value();

		if (ReportType.REDIS == reportType) {
			return new RedisReport(options, name.orElse(RedisReport.DEFAULT_NAME));
		}

		throw new IllegalArgumentException(String.format("\"%s\" is not a valid shared report type.", reportType));
	}
}
