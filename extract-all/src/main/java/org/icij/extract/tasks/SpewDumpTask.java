package org.icij.extract.tasks;

import org.icij.extract.document.Document;
import org.icij.extract.extractor.ExtractionStatus;
import org.icij.extract.report.ReportMap;
import org.icij.extract.report.ReportMapFactory;
import org.icij.extract.report.Reporter;
import org.icij.extract.spewer.SolrSpewer;
import org.icij.spewer.Spewer;
import org.icij.spewer.SpewerFactory;
import org.icij.task.DefaultTask;
import org.icij.task.annotation.OptionsClass;
import org.icij.task.annotation.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;

@Task("Spew a dump file.")
@OptionsClass(SpewerFactory.class)
@OptionsClass(ReportMapFactory.class)
public class SpewDumpTask extends DefaultTask<Void> {

	private static final Logger logger = LoggerFactory.getLogger(SpewTask.class);

	@Override
	public Void run() throws Exception {
		throw new IllegalArgumentException("No paths supplied.");
	}

	@Override
	public Void run(final String[] arguments) throws Exception {
		try (final ReportMap reportMap = new ReportMapFactory(options).create();
		     final Spewer spewer = SpewerFactory.createSpewer(options)) {
			final Reporter reporter = new Reporter(reportMap);

			if (spewer instanceof SolrSpewer) {
				((SolrSpewer) spewer).dump(false);
			}

			for (String path : arguments) {
				logger.info(String.format("Spewing document from \"%s\".", path));

				final Document[] documents = spewer.write(Paths.get(path));

				for (Document document: documents) {
					logger.info(String.format("Spewed \"%s\".", document.getPath()));
					reporter.save(document, ExtractionStatus.SUCCESS);
				}
			}
		}

		return null;
	}
}
