package org.icij.extract.tasks;

import org.icij.extract.report.Report;
import org.icij.extract.tasks.factories.ReportFactory;
import org.icij.task.DefaultTask;
import org.icij.task.annotation.Option;
import org.icij.task.annotation.Task;

/**
 * CLI class for wiping a report from the backend.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
@Task("Wipe a report.")
@Option(name = "report-type", description = "Set the report backend type. For now, the only valid value is" +
		" \"redis\".", parameter = "type", code = "r")
@Option(name = "report-name", description = "The name of the report, the default of which is " +
		"type-dependent.", parameter = "name")
@Option(name = "redis-address", description = "Set the Redis backend address. Defaults to " +
		"127.0.0.1:6379.", parameter = "address")
public class WipeReportTask extends DefaultTask<Integer> {

	@Override
	public Integer run() throws Exception {
		final int cleared;

		try (final Report report = new ReportFactory(options).createShared()) {
			cleared = report.size();
			report.clear();
		} catch (Exception e) {
			throw new RuntimeException("Unexpected exception while wiping report.", e);
		}

		return cleared;
	}
}
