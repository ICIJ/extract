package org.icij.extract.tasks;

import org.icij.extract.document.Document;
import org.icij.extract.report.Report;
import org.icij.extract.report.ReportFactory;

import java.util.Iterator;

import java.nio.file.Files;
import java.nio.file.Path;

import org.icij.task.annotation.Option;
import org.icij.task.annotation.OptionsClass;
import org.icij.task.annotation.Task;
import org.icij.task.MonitorableTask;

/**
 * A command that removes nonexistent file paths from a report, returning the number of paths removed.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
@Task("Remove any files that don't exist on disk from the report.")
@OptionsClass(ReportFactory.class)
public class CleanReportTask extends MonitorableTask<Integer> {

	@Override
	public Integer run() throws Exception {
		int i = 0;

		try (final Report report = new ReportFactory(options).createShared()) {
			final Iterator<Document> iterator = report.keySet().iterator();

			monitor.hintRemaining(report.size());
			while (iterator.hasNext()) {
				Path path = iterator.next().getPath();

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
