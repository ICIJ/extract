package org.icij.extract.tasks;

import org.icij.extract.report.ReportMap;
import org.icij.extract.report.ReportMapFactory;
import org.icij.task.MonitorableTask;
import org.icij.task.annotation.OptionsClass;
import org.icij.task.annotation.Task;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;

/**
 * A command that removes nonexistent file paths from a report, returning the number of paths removed.
 *
 *
 */
@Task("Remove any files that don't exist on disk from the report.")
@OptionsClass(ReportMapFactory.class)
public class CleanReportTask extends MonitorableTask<Integer> {

	@Override
	public Integer call() throws Exception {
		int i = 0;

		try (final ReportMap reportMap = new ReportMapFactory(options).createShared()) {
			final Iterator<Path> iterator = reportMap.keySet().iterator();

			monitor.hintRemaining(reportMap.size());
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
