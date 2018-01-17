package org.icij.extract.tasks;

import com.sun.management.OperatingSystemMXBean;
import org.apache.commons.io.FileUtils;
import org.icij.concurrent.BooleanSealableLatch;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.extractor.DocumentConsumer;
import org.icij.extract.extractor.Extractor;
import org.icij.extract.mysql.DataSourceFactory;
import org.icij.extract.queue.DocumentQueue;
import org.icij.extract.queue.DocumentQueueDrainer;
import org.icij.extract.queue.DocumentQueueFactory;
import org.icij.extract.queue.Scanner;
import org.icij.extract.report.ReportMap;
import org.icij.extract.report.ReportMapFactory;
import org.icij.extract.report.Reporter;
import org.icij.extract.spewer.SpewerFactory;
import org.icij.spewer.Spewer;
import org.icij.task.DefaultTask;
import org.icij.task.annotation.Option;
import org.icij.task.annotation.OptionsClass;
import org.icij.task.annotation.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Spew extracted text from files to an output.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
@Task("Extract from files.")
@OptionsClass(DataSourceFactory.class)
@OptionsClass(DocumentQueueFactory.class)
@OptionsClass(ReportMapFactory.class)
@OptionsClass(Scanner.class)
@OptionsClass(SpewerFactory.class)
@OptionsClass(Extractor.class)
@OptionsClass(DocumentQueueDrainer.class)
@OptionsClass(DocumentFactory.class)
@Option(name = "jobs", description = "The number of documents to process at a time. Defaults to the number" +
		" of available processors.", parameter = "number")
public class SpewTask extends DefaultTask<Long> {

	private static final Logger logger = LoggerFactory.getLogger(SpewTask.class);

	private void checkMemory() {
		final OperatingSystemMXBean os = (com.sun.management.OperatingSystemMXBean)
				ManagementFactory.getOperatingSystemMXBean();
		final long maxMemory = Runtime.getRuntime().maxMemory();

		if (maxMemory < (os.getTotalPhysicalMemorySize() / 4)) {
			logger.warn(String.format("Memory available to JVM (%s) is less than 25%% of available system memory. " +
					"You should probably increase it.", FileUtils.byteCountToDisplaySize(maxMemory)));
		}
	}

	@Override
	public Long run(final String[] paths) throws Exception {
		checkMemory();

		final int parallelism = options.get("jobs").parse().asInteger().orElse(DocumentConsumer.defaultPoolSize());
		final DocumentFactory documentFactory = new DocumentFactory(options);
		final DataSourceFactory dataSourceFactory = new DataSourceFactory(options);

		dataSourceFactory.withMaximumPoolSize(parallelism);

		try (final ReportMap reportMap = new ReportMapFactory(options)
				.withDocumentFactory(documentFactory)
				.create();
			 final DocumentQueue queue = new DocumentQueueFactory(options)
				     .withDocumentFactory(documentFactory)
				     .create();
			 final Spewer spewer = SpewerFactory.createSpewer(options)) {

			return spew(documentFactory, reportMap, spewer, queue, paths, parallelism);
		}
	}

	@Override
	public Long run() throws Exception {
		return run(null);
	}

	private Long spew(final DocumentFactory factory, final ReportMap reportMap, final Spewer spewer, final DocumentQueue
			queue, final String[] paths, final int parallelism) throws Exception {
		logger.info(String.format("Processing up to %d file(s) in parallel.", parallelism));

		final Extractor extractor = new Extractor().configure(options);
		final DocumentConsumer consumer = new DocumentConsumer(spewer, extractor, parallelism);
		final DocumentQueueDrainer drainer = new DocumentQueueDrainer(queue, consumer).configure(options);

		if (null != reportMap) {
			consumer.setReporter(new Reporter(reportMap));
		}

		final Future<Long> draining;
		final Long drained;

		if (null != paths && paths.length > 0) {
			final Scanner scanner = new Scanner(factory, queue, new BooleanSealableLatch(), null).configure(options);
			final List<Future<Path>> scanning = scanner.scan(paths);

			// Set the latch that will be waited on for polling, then start draining in the background.
			drainer.setLatch(scanner.getLatch());
			draining = drainer.drain();

			// Start scanning in a background thread but block until every path has been scanned and queued.
			for (Future<Path> scan : scanning) scan.get();

			// Only a short timeout is needed when awaiting termination, because the call to parse the result of each
			// job is blocking and by the time `awaitTermination` is reached the jobs would have finished.
			scanner.shutdown();
			scanner.awaitTermination(1, TimeUnit.MINUTES);
		} else {

			// Start draining in a background thread.
			draining = drainer.drain();
		}

		// Block until every path in the queue has been consumed.
		drained = draining.get();

		logger.info(String.format("Drained %d files.", drained));

		// Shut down the drainer. Use a short timeout because all jobs should have finished.
		drainer.shutdown();
		drainer.awaitTermination(1, TimeUnit.MINUTES);

		// Use a long timeout because some files might still be processing.
		consumer.shutdown();
		consumer.awaitTermination(7, TimeUnit.DAYS);

		return drained;
	}
}
