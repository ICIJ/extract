package org.icij.extract.tasks;

import org.icij.kaxxa.concurrent.BooleanSealableLatch;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;

import java.lang.management.ManagementFactory;

import com.sun.management.OperatingSystemMXBean;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.extractor.DocumentConsumer;
import org.icij.extract.extractor.Extractor;
import org.icij.extract.queue.*;
import org.icij.extract.report.Report;
import org.icij.extract.report.ReportFactory;
import org.icij.extract.report.Reporter;
import org.icij.extract.spewer.FieldNames;
import org.icij.extract.spewer.Spewer;
import org.icij.extract.spewer.SpewerFactory;
import org.icij.task.DefaultTask;
import org.icij.task.annotation.Option;
import org.icij.task.annotation.OptionsClass;
import org.icij.task.annotation.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spew extracted text from files to an output.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
@Task("Extract from files.")
@OptionsClass(DocumentQueueFactory.class)
@OptionsClass(ReportFactory.class)
@OptionsClass(Scanner.class)
@OptionsClass(SpewerFactory.class)
@OptionsClass(Extractor.class)
@OptionsClass(FieldNames.class)
@OptionsClass(DocumentQueueDrainer.class)
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

		final DocumentFactory factory = new DocumentFactory().configure(options);

		try (final Report report = new ReportFactory(options)
				.withDocumentFactory(factory)
				.create();
		     final Spewer spewer = SpewerFactory.createSpewer(options);
		     final DocumentQueue queue = new DocumentQueueFactory(options)
				     .withDocumentFactory(factory)
				     .create()) {

			return spew(factory, report, spewer, queue, paths);
		}
	}

	@Override
	public Long run() throws Exception {
		return run(null);
	}

	private Long spew(final DocumentFactory factory, final Report report, final Spewer spewer, final DocumentQueue
			queue, final String[] paths) throws Exception {
		final int parallelism = options.get("jobs").parse().asInteger().orElse(DocumentConsumer.defaultPoolSize());
		logger.info(String.format("Processing up to %d file(s) in parallel.", parallelism));

		final Extractor extractor = new Extractor().configure(options);
		final DocumentConsumer consumer = new DocumentConsumer(spewer, extractor, parallelism);
		final DocumentQueueDrainer drainer = new DocumentQueueDrainer(queue, consumer).configure(options);

		if (null != report) {
			consumer.setReporter(new Reporter(report));
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
