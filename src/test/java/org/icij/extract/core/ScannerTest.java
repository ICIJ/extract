package org.icij.extract.core;

import org.icij.extract.test.*;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

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

		try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(root)) {
			for (Path file : directoryStream) {
				Assert.assertTrue(String.format("Failed asserting that queue contains \"%s\".", file),
						queue.contains(file));
			}
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

		try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(root)) {
			for (Path file : directoryStream) {
				Path fileName = file.getFileName();
				Assert.assertTrue(String.format("Failed asserting that queue contains \"%s\".", fileName),
						queue.contains(fileName));
			}
		}
	}
}
