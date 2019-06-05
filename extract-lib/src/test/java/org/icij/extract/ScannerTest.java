package org.icij.extract;

import org.icij.concurrent.BooleanSealableLatch;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.PathIdentifier;
import org.icij.extract.queue.ArrayDocumentQueue;
import org.icij.extract.queue.DocumentQueue;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class ScannerTest {

	private final DocumentFactory factory = new DocumentFactory().withIdentifier(new PathIdentifier());
	private final DocumentQueue queue = new ArrayDocumentQueue(100);
	private Scanner scanner = new Scanner(factory, queue);

	@After
	public void tearDown() throws InterruptedException {
		queue.clear();
	}

	@Test
	public void testScanDirectory() throws Throwable {
		final Path root = Paths.get(getClass().getResource("/documents/text/").toURI());

		// Block until every single path has been scanned and queued.
		final Future<Path> job = scanner.scan(root);

		assertEquals(job.get(), root);
		shutdownScanner(scanner);

		// Assert that the queue contains at least one file, manually.
		Assert.assertTrue(Files.exists(root.resolve("plain.txt")));
		Assert.assertTrue(String.format("Queued file path must start with root path \"%s\".", root),
				queue.contains(factory.create(root.resolve("plain.txt"))));

		try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(root)) {
			for (Path file : directoryStream) {
				Assert.assertTrue(String.format("Failed asserting that queue contains \"%s\".", file),
						queue.contains(factory.create(file)));
			}
		}
	}

	@Test
	public void testScanNumberOfFiles() throws Exception {
		final Path regularFiles = Paths.get(getClass().getResource("/documents/text/").toURI());
		assertEquals(3, scanner.getNumberOfFiles(regularFiles));

		scanner.ignoreSystemFiles(true);
		final Path rootFiles = Paths.get(getClass().getResource("/documents/").toURI());
		assertEquals(9, scanner.getNumberOfFiles(rootFiles));

		shutdownScanner(scanner);
	}

	@Test
	public void testScanDirectoryWithIncludeGlob() throws Throwable {
		final Path root = Paths.get(getClass().getResource("/documents/").toURI());

		scanner.include("**.txt");

		// Block until every single path has been scanned and queued.
		final Future<Path> job = scanner.scan(root);

		assertEquals(job.get(), root);
		shutdownScanner(scanner);

		// Assert that the queue contains at least one file, manually.
		final Path garbage = root.resolve("garbage.bin");

		Assert.assertTrue(Files.exists(garbage));
		Assert.assertFalse(queue.contains(factory.create(garbage)));
		Assert.assertTrue(queue.contains(factory.create(root.resolve("text/plain.txt"))));
	}

	@Test
	public void testScanDirectoryWithExcludeGlob() throws Throwable {
		final Path root = Paths.get(getClass().getResource("/documents/").toURI());

		// Test exclude paths by extension.
		scanner.exclude("**.bin");

		// Test excluding directories.
		scanner.exclude("**/ocr");

		// Block until every single path has been scanned and queued.
		final Future<Path> job = scanner.scan(root);

		assertEquals(job.get(), root);
		shutdownScanner(scanner);

		// Assert that the queue contains at least one file, manually.
		final Path garbage = root.resolve("garbage.bin");
		final Path ocrTiff = root.resolve("ocr/simple.tiff");

		// Test paths excluded by extension.
		Assert.assertTrue(Files.exists(garbage));
		Assert.assertFalse(queue.contains(factory.create(garbage)));

		// Test whether entire directory was excluded.
		Assert.assertTrue(Files.exists(ocrTiff));
		Assert.assertFalse(queue.contains(factory.create(ocrTiff)));

		// Test whether at least one other file was included.
		Assert.assertTrue(queue.contains(factory.create(root.resolve("text/plain.txt"))));
	}

	@Test
	public void testHandlesSymlink() throws Throwable {
		final Path root = Paths.get(getClass().getResource("/links/").toURI());

		scanner.followSymLinks(true);
		Assert.assertTrue(scanner.followSymLinks());

		// Create the link.
		final Path documents = root.resolve("documents");

		if (Files.notExists(documents)) {
			Files.createSymbolicLink(documents, root.resolve("../documents"));
		}

		final Future<Path> job = scanner.scan(root);

		assertEquals(job.get(), root);
		shutdownScanner(scanner);

		// Assert that the queue doesn't contain the symlink, but contains linked files.
		Assert.assertTrue(Files.isSymbolicLink(documents));
		Assert.assertTrue(Files.exists(documents));
		Assert.assertFalse(queue.contains(factory.create(documents)));
		Assert.assertTrue(queue.contains(factory.create(root.resolve("documents/garbage.bin"))));
	}

	@Test
	public void testIgnoreHiddenFiles() throws Throwable {
		final Path root = Paths.get(getClass().getResource("/documents/").toURI());
		final Path hidden = root.resolve(".hidden");

		scanner.ignoreHiddenFiles(true);
		Assert.assertTrue(scanner.ignoreHiddenFiles());

		// Block until every single path has been scanned and queued.
		assertEquals(scanner.scan(root).get(), root);

		// Assert that the queue does not contain the hidden file.
		Assert.assertTrue(Files.exists(hidden));
		Assert.assertFalse(queue.contains(factory.create(hidden)));

		// Now test if hidden files are scanned.
		scanner.ignoreHiddenFiles(false);
		Assert.assertFalse(scanner.ignoreHiddenFiles());

		assertEquals(scanner.scan(root).get(), root);
		Assert.assertTrue(queue.contains(factory.create(hidden)));
		shutdownScanner(scanner);
	}

	@Test
	public void testIgnoresSystemFilesByDefault() throws Throwable {
		final Path root = Paths.get(getClass().getResource("/documents/").toURI());
		final Path system = root.resolve("lost+found/trashed");

		Assert.assertTrue(scanner.ignoreSystemFiles());

		// Block until every single path has been scanned and queued.
		assertEquals(scanner.scan(root).get(), root);

		// Assert that the queue does not contain the system file.
		Assert.assertTrue(Files.exists(system));
		Assert.assertFalse(queue.contains(factory.create(system)));

		// Now test if system files are scanned.
		scanner.ignoreSystemFiles(false);
		Assert.assertFalse(scanner.ignoreSystemFiles());

		assertEquals(scanner.scan(root).get(), root);
		Assert.assertTrue(queue.contains(factory.create(system)));
		shutdownScanner(scanner);
	}

	@Test
	public void testMaxDepth() throws Throwable {
		final Path root = Paths.get(getClass().getResource("/documents/").toURI());

		scanner.setMaxDepth(1);
		assertEquals(1, scanner.getMaxDepth());

		// Block until every single path has been scanned and queued.
		assertEquals(scanner.scan(root).get(), root);

		Assert.assertTrue(Files.exists(root.resolve("text/plain.txt")));
		Assert.assertFalse(queue.contains(factory.create(root.resolve("text/plain.txt"))));
		Assert.assertTrue(queue.contains(factory.create(root.resolve("garbage.bin"))));
		shutdownScanner(scanner);
	}

	@Test
	public void testLatch() throws Throwable {
		final Path root = Paths.get(getClass().getResource("/documents/").toURI());
		final Path garbage = root.resolve("garbage.bin");
		final Scanner scanner = new Scanner(factory, queue, new BooleanSealableLatch());

		new Timer().schedule(new TimerTask() {

			@Override
			public void run() {
				scanner.scan(garbage);
			}
		}, 1000);

		scanner.getLatch().await();

		Assert.assertTrue(Files.exists(garbage));
		Assert.assertTrue(queue.contains(factory.create(garbage)));

		shutdownScanner(scanner);
	}

	private void shutdownScanner(final Scanner scanner) throws InterruptedException {
		scanner.shutdown();
		scanner.awaitTermination(1, TimeUnit.SECONDS);
	}
}
