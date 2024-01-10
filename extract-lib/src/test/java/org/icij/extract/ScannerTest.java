package org.icij.extract;

import org.icij.concurrent.BooleanSealableLatch;
import org.icij.extract.queue.MemoryDocumentQueue;
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
import java.util.stream.Collectors;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

public class ScannerTest {
	private final DocumentQueue<Path> queue = new MemoryDocumentQueue<>("extract:queue", 100);
	private final Scanner scanner = new Scanner(queue);

	@Test
	public void testScanDirectory() throws Throwable {
		final Path root = Paths.get(getClass().getResource("/documents/text/").toURI());

		assertEquals(3L, (long)scanner.scan(root).get());

		Assert.assertTrue(Files.exists(root.resolve("plain.txt")));
		Assert.assertTrue(String.format("Queued file path must start with root path \"%s\".", root),
				queue.contains(root.resolve("plain.txt")));

		try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(root)) {
			for (Path file : directoryStream) {
				Assert.assertTrue(String.format("Failed asserting that queue contains \"%s\".", file),
						queue.contains(file));
			}
		}
	}

	@Test
	public void testScanNumberOfFiles() throws Exception {
		final Path regularFiles = Paths.get(getClass().getResource("/documents/text/").toURI());
		assertEquals(3, scanner.getNumberOfFiles(regularFiles));

		scanner.ignoreSystemFiles(true);
		final Path rootFiles = Paths.get(getClass().getResource("/documents/").toURI());
		assertEquals(13, scanner.getNumberOfFiles(rootFiles));
	}

	@Test
	public void testScanDirectoryWithIncludeGlob() throws Throwable {
		final Path root = Paths.get(getClass().getResource("/documents/").toURI());
		scanner.include("**.txt");

		final Future<Long> job = scanner.scan(root);

		assertEquals(2, (long)job.get());
		Assert.assertTrue(queue.contains(root.resolve("text/plain.txt")));
		Assert.assertTrue(queue.contains(root.resolve("text/utf16.txt")));
	}

	@Test
	public void testScanDirectoryWithExcludeGlob() throws Throwable {
		final Path root = Paths.get(getClass().getResource("/documents/").toURI());

		// Test exclude paths by extension.
		scanner.exclude("**.bin");
		// Test excluding directories.
		scanner.exclude("**/ocr");
		scanner.scan(root).get();

		final Path garbage = root.resolve("garbage.bin");
		Assert.assertTrue(Files.exists(garbage));
		Assert.assertFalse(queue.contains(garbage));

		final Path ocrTiff = root.resolve("ocr/simple.tiff");
		Assert.assertTrue(Files.exists(ocrTiff));
		Assert.assertFalse(queue.contains(ocrTiff));

		Assert.assertTrue(queue.contains(root.resolve("text/plain.txt")));
	}

	@Test
	public void testHandlesSymlink() throws Throwable {
		final Path root = Paths.get(getClass().getResource("/links/").toURI());

		scanner.followSymLinks(true);
		Assert.assertTrue(scanner.followSymLinks());

		final Path documents = root.resolve("documents");
		if (Files.notExists(documents)) {
			Files.createSymbolicLink(documents, root.resolve("../documents"));
		}

		scanner.scan(root).get();

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

		scanner.ignoreHiddenFiles(true);
		scanner.scan(root).get();

		Assert.assertTrue(Files.exists(hidden));
		Assert.assertFalse(queue.contains(hidden));

		scanner.ignoreHiddenFiles(false);
		scanner.scan(root).get();
		Assert.assertTrue(queue.contains(hidden));
	}

	@Test
	public void testIgnoresSystemFilesByDefault() throws Throwable {
		final Path root = Paths.get(getClass().getResource("/documents/").toURI());
		final Path system = root.resolve("lost+found/trashed");

		Assert.assertTrue(scanner.ignoreSystemFiles());
		scanner.scan(root).get();

		// Assert that the queue does not contain the system file.
		Assert.assertTrue(Files.exists(system));
		Assert.assertFalse(queue.contains(system));

		// Now test if system files are scanned.
		scanner.ignoreSystemFiles(false);
		scanner.scan(root).get();
		Assert.assertTrue(queue.contains(system));
	}

	@Test
	public void testMaxDepth() throws Throwable {
		final Path root = Paths.get(getClass().getResource("/documents/").toURI());
		scanner.setMaxDepth(1);

		scanner.scan(root).get();

		for(Path p: queue.stream().collect(Collectors.toList())) {
			assertThat(root.relativize(p).toString().contains("/")).isFalse();
		}
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

	@After
	public void tearDown() throws InterruptedException {
		queue.clear();
		shutdownScanner(scanner);
	}

	private void shutdownScanner(final Scanner scanner) throws InterruptedException {
		scanner.shutdown();
		scanner.awaitTermination(1, TimeUnit.SECONDS);
	}

}
