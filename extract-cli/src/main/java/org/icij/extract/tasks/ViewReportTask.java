package org.icij.extract.tasks;

import org.icij.extract.document.TikaDocument;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.report.Report;
import org.icij.extract.report.ReportMap;
import org.icij.extract.report.ReportMapFactory;
import org.icij.task.DefaultTask;
import org.icij.task.annotation.OptionsClass;
import org.icij.task.annotation.Task;

/**
 * CLI class for viewing a single report.
 */
@Task("View a single report.")
@OptionsClass(ReportMapFactory.class)
public class ViewReportTask extends DefaultTask<Void> {

	@Override
	public Void call() throws Exception {
		throw new RuntimeException("No ID or path supplied.");
	}

	@Override
	public Void call(final String[] arguments) throws Exception {
		final ReportMapFactory reportMapFactory = new ReportMapFactory(options);
		final DocumentFactory documentFactory = new DocumentFactory().configure(options);
		reportMapFactory.withDocumentFactory(documentFactory);

		try (final ReportMap reportMap = reportMapFactory.createShared()) {
			for (String path: arguments) {
				final TikaDocument tikaDocument = documentFactory.create(path);
				final Report report = reportMap.get(tikaDocument);

				System.out.println("Status:");
				System.out.println(report.getStatus());

				report.getException().ifPresent(e -> {
					System.out.println("Exception:");
					e.printStackTrace(System.out);
				});
			}
		} catch (Exception e) {
			throw new RuntimeException("Unexpected exception while viewing report.", e);
		}

		return null;
	}
}
