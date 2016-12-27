package org.icij.extract.tasks;

import org.icij.extract.report.Report;
import org.icij.extract.report.ReportFactory;
import org.icij.task.DefaultTask;
import org.icij.task.annotation.Option;
import org.icij.task.annotation.OptionsClass;
import org.icij.task.annotation.Task;

/**
 * CLI class for wiping a report from the backend.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
@Task("Wipe a report.")
@OptionsClass(ReportFactory.class)
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
