package org.icij.extract.tasks;

import org.icij.extract.report.Report;
import org.icij.extract.tasks.factories.ReportFactory;

import java.util.Iterator;

import java.nio.file.Files;
import java.nio.file.Path;

import org.icij.task.annotation.Option;
import org.icij.task.annotation.Task;
import org.icij.task.MonitorableTask;

/**
 * A command that removes nonexistent file paths from a report, returning the number of paths removed.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
@Task("Remove any files that don't exist on disk from the report.")
@Option(name = "report-type", description = "Set the report backend type. For now, the only valid value is" +
		" \"redis\".", parameter = "type", code = "r")
@Option(name = "report-name", description = "The name of the report, the default of which is " +
		"type-dependent.", parameter = "name")
@Option(name = "redis-address", description = "Set the Redis backend address. Defaults to " +
		"127.0.0.1:6379.", parameter = "address")
public class CleanReportTask extends MonitorableTask<Integer> {

	@Override
	public Integer run() throws Exception {
		int i = 0;

		try (final Report report = new ReportFactory(options).createShared()) {
			final Iterator<Path> iterator = report.keySet().iterator();

			monitor.hintRemaining(report.size());
			while (iterator.hasNext()) {
				Path path = iterator.next();

				if (Files.notExists(path)) {
					iterator.remove();
					i++;
				}

				monitor.notifyListeners();
			}
		} catch (Exception e) {
			throw new RuntimeException("Unexpected exception while cleaning report.", e);
		}

		return i;
	}
}
