package org.icij.extract.core;

import java.nio.file.*;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.icij.concurrent.BooleanSealableLatch;
import org.icij.extract.queue.ArrayPathQueue;
import org.icij.extract.queue.PathQueue;
import org.icij.extract.queue.Scanner;
import org.junit.*;

public class ScannerTest {

	private final PathQueue queue = new ArrayPathQueue(100);

	private Scanner createScanner() {
		return new Scanner(queue);
	}

	private void shutdownScanner(final Scanner scanner) throws InterruptedException {
		scanner.shutdown();
		scanner.awaitTermination(1, TimeUnit.SECONDS);
	}

	@After
	public void tearDown() throws InterruptedException {
		queue.clear();
	}

	@Test
	public void testScanDirectory() throws Throwable {
		final Path root = Paths.get(getClass().getResource("/documents/text/").toURI());
		final Scanner scanner = createScanner();

		// Block until every single path has been scanned and queued.
		final Future<Path> job = scanner.scan(root);

		Assert.assertEquals(job.get(), root);
		shutdownScanner(scanner);

		// Assert that the queue contains at least one file, manually.
		Assert.assertTrue(Files.exists(root.resolve("plain.txt")));
		Assert.assertTrue(String.format("Queued file path must start with root path \"%s\".", root), queue.contains(root
				.resolve("plain.txt")));

		try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(root)) {
			for (Path file : directoryStream) {
				Assert.assertTrue(String.format("Failed asserting that queue contains \"%s\".", file),
						queue.contains(file));
			}
		}
	}

	@Test
	public void testScanDirectoryWithBase() throws Throwable {
		final Path root = Paths.get(getClass().getResource("/documents/text/").toURI());
		final Scanner scanner = createScanner();
		final Future<Path> job = scanner.scan(root, root);

		Assert.assertEquals(job.get(), root);
		shutdownScanner(scanner);

		// Assert that the queue contains at least one file, manually.
		Assert.assertTrue(Files.exists(root.resolve("plain.txt")));
		Assert.assertTrue(queue.contains(Paths.get("plain.txt")));

		// Assert that the queue contains the basename (the name of the file without the directory component) of every
		// file in the directory.
		try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(root)) {
			for (Path file : directoryStream) {
				Path fileName = file.getFileName();
				Assert.assertTrue(String.format("Failed asserting that queue contains \"%s\".", fileName),
						queue.contains(fileName));
			}
		}
	}

	@Test
	public void testScanDirectoryWithIncludeGlob() throws Throwable {
		final Path root = Paths.get(getClass().getResource("/documents/").toURI());
		final Scanner scanner = createScanner();

		scanner.include("**.txt");

		// Block until every single path has been scanned and queued.
		final Future<Path> job = scanner.scan(root);

		Assert.assertEquals(job.get(), root);
		shutdownScanner(scanner);

		// Assert that the queue contains at least one file, manually.
		final Path garbage = root.resolve("garbage.bin");

		Assert.assertTrue(Files.exists(garbage));
		Assert.assertFalse(queue.contains(garbage));
		for (Object path : queue.toArray()) {
			System.out.println("PATH: " + path.toString());
		}

		Assert.assertTrue(queue.contains(root.resolve("text/plain.txt")));
	}

	@Test
	public void testScanDirectoryWithExcludeGlob() throws Throwable {
		final Path root = Paths.get(getClass().getResource("/documents/").toURI());
		final Scanner scanner = createScanner();

		// Test exclude paths by extension.
		scanner.exclude("**.bin");

		// Test excluding directories.
		scanner.exclude("**/ocr");

		// Block until every single path has been scanned and queued.
		final Future<Path> job = scanner.scan(root);

		Assert.assertEquals(job.get(), root);
		shutdownScanner(scanner);

		// Assert that the queue contains at least one file, manually.
		final Path garbage = root.resolve("garbage.bin");
		final Path ocrTiff = root.resolve("ocr/simple.tiff");

		// Test paths excluded by extension.
		Assert.assertTrue(Files.exists(garbage));
		Assert.assertFalse(queue.contains(garbage));

		// Test whether entire directory was excluded.
		Assert.assertTrue(Files.exists(ocrTiff));
		Assert.assertFalse(queue.contains(ocrTiff));

		// Test whether at least one other file was included.
		Assert.assertTrue(queue.contains(root.resolve("text/plain.txt")));
	}

	@Test
	public void testHandlesSymlink() throws Throwable {
		final Path root = Paths.get(getClass().getResource("/links/").toURI());
		final Scanner scanner = createScanner();

		scanner.followSymLinks(true);
		Assert.assertTrue(scanner.followSymLinks());

		// Create the link.
		final Path documents = root.resolve("documents");

		if (Files.notExists(documents)) {
			Files.createSymbolicLink(documents, root.resolve("../documents"));
		}

		final Future<Path> job = scanner.scan(root);

		Assert.assertEquals(job.get(), root);
		shutdownScanner(scanner);

		// Assert that the queue doesn't contain the symlink, but contains linked files.
		Assert.assertTrue(Files.isSymbolicLink(documents));
		Assert.assertTrue(Files.exists(documents));
		Assert.assertFalse(queue.contains(documents));
		Assert.assertTrue(queue.contains(root.resolve("documents/garbage.bin")));
	}

	@Test
	public void testIgnoreHiddenFiles() throws Throwable {
		final Path root = Paths.get(getClass().getResource("/documents/").toURI());
		final Path hidden = root.resolve(".hidden");
		final Scanner scanner = createScanner();

		scanner.ignoreHiddenFiles(true);
		Assert.assertTrue(scanner.ignoreHiddenFiles());

		// Block until every single path has been scanned and queued.
		Assert.assertEquals(scanner.scan(root).get(), root);

		// Assert that the queue does not contain the hidden file.
		Assert.assertTrue(Files.exists(hidden));
		Assert.assertFalse(queue.contains(hidden));

		// Now test if hidden files are scanned.
		scanner.ignoreHiddenFiles(false);
		Assert.assertFalse(scanner.ignoreHiddenFiles());

		Assert.assertEquals(scanner.scan(root).get(), root);
		Assert.assertTrue(queue.contains(hidden));
		shutdownScanner(scanner);
	}

	@Test
	public void testIgnoresSystemFilesByDefault() throws Throwable {
		final Path root = Paths.get(getClass().getResource("/documents/").toURI());
		final Path system = root.resolve("lost+found/trashed");
		final Scanner scanner = createScanner();

		Assert.assertTrue(scanner.ignoreSystemFiles());

		// Block until every single path has been scanned and queued.
		Assert.assertEquals(scanner.scan(root).get(), root);

		// Assert that the queue does not contain the system file.
		Assert.assertTrue(Files.exists(system));
		Assert.assertFalse(queue.contains(system));

		// Now test if system files are scanned.
		scanner.ignoreSystemFiles(false);
		Assert.assertFalse(scanner.ignoreSystemFiles());

		Assert.assertEquals(scanner.scan(root).get(), root);
		Assert.assertTrue(queue.contains(system));
		shutdownScanner(scanner);
	}

	@Test
	public void testMaxDepth() throws Throwable {
		final Path root = Paths.get(getClass().getResource("/documents/").toURI());
		final Scanner scanner = createScanner();

		scanner.setMaxDepth(1);
		Assert.assertEquals(1, scanner.getMaxDepth());

		// Block until every single path has been scanned and queued.
		Assert.assertEquals(scanner.scan(root).get(), root);

		Assert.assertTrue(Files.exists(root.resolve("text/plain.txt")));
		Assert.assertFalse(queue.contains(root.resolve("text/plain.txt")));
		Assert.assertTrue(queue.contains(root.resolve("garbage.bin")));
		shutdownScanner(scanner);
	}

	@Test
	public void testLatch() throws Throwable {
		final Path root = Paths.get(getClass().getResource("/documents/").toURI());
		final Path garbage = root.resolve("garbage.bin");
		final Scanner scanner = new Scanner(queue, new BooleanSealableLatch());

		new Timer().schedule(new TimerTask() {

			@Override
			public void run() {
				scanner.scan(garbage);
			}
		}, 1000);

		scanner.getLatch().await();

		Assert.assertTrue(Files.exists(garbage));
		Assert.assertTrue(queue.contains(garbage));

		shutdownScanner(scanner);
	}
}
