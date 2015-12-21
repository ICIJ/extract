package org.icij.extract.cli.factory;

import org.icij.extract.core.Report;
import org.icij.extract.core.ReportType;
import org.icij.extract.redis.RedisReport;

import org.apache.commons.cli.CommandLine;

/**
 * Factory methods for creating {@link Report} objects.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public class ReportFactory {

	public static Report createReport(final CommandLine cmd) {
		final ReportType reportType = ReportType.parse(cmd.getOptionValue('r', "none"));
		final String name = cmd.getOptionValue("report-name");
		final Report report;

		if (ReportType.REDIS == reportType) {
			report = RedisReport.create(RedisConfigFactory.createConfig(cmd), name);
		} else {
			report = null;
		}

		return report;
	}
}
