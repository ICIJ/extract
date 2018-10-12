package org.icij.extract.tasks;

import org.icij.extract.report.ReportMap;
import org.icij.extract.report.ReportMapFactory;
import org.icij.task.DefaultTask;
import org.icij.task.annotation.OptionsClass;
import org.icij.task.annotation.Task;

/**
 * CLI class for wiping a report from the backend.
 *
 *
 */
@Task("Wipe a report.")
@OptionsClass(ReportMapFactory.class)
public class WipeReportTask extends DefaultTask<Integer> {

	@Override
	public Integer call() throws Exception {
		final int cleared;

		try (final ReportMap reportMap = new ReportMapFactory(options).createShared()) {
			cleared = reportMap.size();
			reportMap.clear();
		} catch (Exception e) {
			throw new RuntimeException("Unexpected exception while wiping report.", e);
		}

		return cleared;
	}
}
