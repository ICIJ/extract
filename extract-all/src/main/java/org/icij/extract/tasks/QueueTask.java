package org.icij.extract.tasks;

import org.icij.extract.document.DocumentFactory;
import org.icij.extract.queue.DocumentQueue;
import org.icij.extract.queue.Scanner;

import org.icij.extract.queue.DocumentQueueFactory;

import java.nio.file.Path;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.icij.task.MonitorableTask;
import org.icij.task.annotation.OptionsClass;
import org.icij.task.annotation.Task;

/**
 * Task that scans paths for files to add to a queue.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
@Task("Queue files for processing later.")
@OptionsClass(DocumentQueueFactory.class)
@OptionsClass(Scanner.class)
public class QueueTask extends MonitorableTask<Long> {

	@Override
	public Long call(final String[] paths) throws Exception {
		if (null == paths || paths.length == 0) {
			throw new IllegalArgumentException("You must pass the paths to scan on the command line.");
		}

		final DocumentFactory factory = new DocumentFactory().configure(options);

		try (final DocumentQueue queue = new DocumentQueueFactory(options)
				.withDocumentFactory(factory)
				.createShared()) {
			return queue(new Scanner(factory, queue, null, monitor).configure(options), paths);
		}
	}

	@Override
	public Long call() throws Exception {
		final String[] paths = new String[1];

		paths[0] = ".";
		return call(paths);
	}

	/**
	 * Submit the given list of paths to the scanner.
	 *
	 * @param scanner the scanner to submit paths to
	 * @param paths the paths to scan
	 * @throws InterruptedException if interrupted while waiting for a scan to complete
	 * @throws ExecutionException if an exception occurs while scanning
	 */
	private long queue(final Scanner scanner, final String... paths) throws InterruptedException, ExecutionException {

		// Block until each scan has completed. These jobs will complete in serial or parallel depending on the
		// executor used by the scanner.
		for (Future<Path> scan : scanner.scan(paths)) {
			scan.get();
		}

		// Shut down the scanner and block until the scanning of each directory has completed in serial.
		// Note that each scan operation is completed in serial before this point is reached, so only a short timeout
		// is needed.
		scanner.shutdown();
		scanner.awaitTermination(1, TimeUnit.MINUTES);
		return scanner.queued();
	}
}
