package org.icij.extract.core;

import org.icij.extract.test.*;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.junit.Assert;

public class ScannerTest extends TestBase {

	@Test
	public void testScanDirectory() throws Throwable {
		final Queue queue = ArrayQueue.create(100);
		final Scanner scanner = new Scanner(logger, queue);
		final Path root = Paths.get(getClass().getResource("/documents/text/").toURI());

		scanner.scan(root);

		// Block until every single path has been scanned and queued.
		scanner.shutdown();
		scanner.awaitTermination();

		final List<File> files = Arrays.asList(root.toFile().listFiles());
		for (File file : files) {
			Assert.assertTrue(String.format("Failed asserting that queue contains \"%s\".", file),
					queue.contains(file.toPath()));
		}
	}

	@Test
	public void testScanDirectoryWithBase() throws Throwable {
		final Queue queue = ArrayQueue.create(100);
		final Scanner scanner = new Scanner(logger, queue);
		final Path root = Paths.get(getClass().getResource("/documents/text/").toURI());

		scanner.scan(root, root);

		// Block until every single path has been scanned and queued.
		scanner.shutdown();
		scanner.awaitTermination();

		final List<File> files = Arrays.asList(root.toFile().listFiles());
		for (File file : files) {
			Assert.assertTrue(String.format("Failed asserting that queue contains \"%s\".", file),
					queue.contains(file.toPath().getFileName()));
		}
	}
}
