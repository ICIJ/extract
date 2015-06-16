package org.icij.extract.core;

import java.util.logging.Logger;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import java.nio.file.Path;
import java.nio.file.Paths;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Test;
import org.junit.Assert;

public class ConsumerTest {

	@Test
	public void testConsume() throws Throwable {
		final Logger logger = Logger.getLogger("extract-test");

		final Extractor extractor = new Extractor(logger);

		final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		final PrintStream printStream = new PrintStream(outputStream);
		final Spewer spewer = new PrintStreamSpewer(logger, printStream);

		final int threads = 2;
		final Consumer consumer = new Consumer(logger, spewer, extractor, threads);

		final Path file = Paths.get(getClass().getResource("/documents/text/plain.txt").toURI());

		consumer.consume(file);
		consumer.awaitTermination();

		Assert.assertEquals("This is a test.\n\n", outputStream.toString());
	}

	@Test
	public void testConsumeWithQueue() throws Throwable {
		final Logger logger = Logger.getLogger("extract-test");

		final Extractor extractor = new Extractor(logger);

		final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		final PrintStream printStream = new PrintStream(outputStream);
		final Spewer spewer = new PrintStreamSpewer(logger, printStream);

		final int threads = 2;
		final BlockingQueue<String> queue = new ArrayBlockingQueue<String>(threads * 2);
		final PollingConsumer consumer = new PollingConsumer(logger, queue, spewer, extractor, threads);

		final Path file = Paths.get(getClass().getResource("/documents/text/plain.txt").toURI());

		queue.put(file.toString());
		consumer.start();
		consumer.awaitTermination();

		Assert.assertEquals("This is a test.\n\n", outputStream.toString());
	}

	@Test
	public void testConsumeWithScanner() throws Throwable {
		final Logger logger = Logger.getLogger("extract-test");

		final Extractor extractor = new Extractor(logger);

		final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		final PrintStream printStream = new PrintStream(outputStream);
		final Spewer spewer = new PrintStreamSpewer(logger, printStream);

		final int threads = 2;
		final BlockingQueue<String> queue = new ArrayBlockingQueue<String>(threads * 2);
		final PollingConsumer consumer = new PollingConsumer(logger, queue, spewer, extractor, threads);

		final ExecutorService scan = Executors.newSingleThreadExecutor();

		scan.submit(new QueueingScanner(logger, queue, Paths.get(getClass().getResource("/documents/text/plain.txt").toURI())));
		scan.submit(new QueueingScanner(logger, queue, Paths.get(getClass().getResource("/documents/ocr/simple.tiff").toURI())));

		consumer.start();
		consumer.awaitTermination();

		Assert.assertEquals("This is a test.\n\nHEAVY\nMETAL\n\n\n", outputStream.toString());
	}

	@Test
	public void testConsumeWithDirectoryScanner() throws Throwable {
		final Logger logger = Logger.getLogger("extract-test");

		final Extractor extractor = new Extractor(logger);

		final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		final PrintStream printStream = new PrintStream(outputStream);
		final Spewer spewer = new PrintStreamSpewer(logger, printStream);

		final int threads = 2;
		final BlockingQueue<String> queue = new ArrayBlockingQueue<String>(threads * 2);
		final PollingConsumer consumer = new PollingConsumer(logger, queue, spewer, extractor, threads);

		final ExecutorService scan = Executors.newSingleThreadExecutor();

		scan.submit(new QueueingScanner(logger, queue, Paths.get(getClass().getResource("/documents/text/").toURI())));

		consumer.start();
		consumer.awaitTermination();

		Assert.assertEquals("This is a test.\n\n", outputStream.toString());
	}
}
