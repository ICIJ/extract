package org.icij.extract.core;

import java.util.logging.Logger;
import java.util.logging.Level;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import java.nio.file.Path;
import java.nio.file.Paths;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;

import org.redisson.Redisson;
import com.lambdaworks.redis.RedisConnectionException;

import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.Assert;
import org.junit.Assume;

public class ConsumerTest {

	public static final Logger logger = Logger.getLogger("extract:test");

	@BeforeClass
	public static void setUpBeforeClass() {
		logger.setLevel(Level.INFO);
	}

	@Test
	public void testConsume() throws Throwable {
		final Extractor extractor = new Extractor(logger);

		final ByteArrayOutputStream output = new ByteArrayOutputStream();
		final PrintStream print = new PrintStream(output);
		final Spewer spewer = new PrintStreamSpewer(logger, print);

		final int threads = 2;
		final Consumer consumer = new Consumer(logger, spewer, extractor, threads);

		final Path file = Paths.get(getClass().getResource("/documents/text/plain.txt").toURI());

		consumer.consume(file);
		consumer.awaitTermination();

		Assert.assertEquals("This is a test.\n\n", output.toString());
	}

	@Test
	public void testConsumeWithQueue() throws Throwable {
		final Extractor extractor = new Extractor(logger);

		final ByteArrayOutputStream output = new ByteArrayOutputStream();
		final PrintStream print = new PrintStream(output);
		final Spewer spewer = new PrintStreamSpewer(logger, print);

		final int threads = 2;
		final BlockingQueue<String> queue = new ArrayBlockingQueue<String>(threads * 2);
		final PollingConsumer consumer = new PollingConsumer(logger, queue, spewer, extractor, threads);

		final Path file = Paths.get(getClass().getResource("/documents/text/plain.txt").toURI());

		queue.put(file.toString());
		consumer.start();
		consumer.awaitTermination();

		Assert.assertEquals("This is a test.\n\n", output.toString());
	}

	@Test
	public void testConsumeWithRedisQueue() throws Throwable {
		final Extractor extractor = new Extractor(logger);

		final ByteArrayOutputStream output = new ByteArrayOutputStream();
		final PrintStream print = new PrintStream(output);
		final Spewer spewer = new PrintStreamSpewer(logger, print);

		final int threads = 2;

		try {
			final Redisson redisson = Redisson.create();
			final BlockingQueue<String> queue = redisson.getBlockingQueue("extract:test:queue");
			final PollingConsumer consumer = new PollingConsumer(logger, queue, spewer, extractor, threads);

			final Path file = Paths.get(getClass().getResource("/documents/text/plain.txt").toURI());

			queue.put(file.toString());
			consumer.start();
			consumer.awaitTermination();

			redisson.shutdown();
		} catch (RedisConnectionException e) {
			Assume.assumeNoException(e);
			return;
		}

		Assert.assertEquals("This is a test.\n\n", output.toString());
	}

	@Test
	public void testConsumeWithScanner() throws Throwable {
		final Extractor extractor = new Extractor(logger);

		final ByteArrayOutputStream output = new ByteArrayOutputStream();
		final PrintStream print = new PrintStream(output);
		final Spewer spewer = new PrintStreamSpewer(logger, print);

		final int threads = 2;
		final BlockingQueue<String> queue = new ArrayBlockingQueue<String>(threads * 2);
		final PollingConsumer consumer = new PollingConsumer(logger, queue, spewer, extractor, threads);
		final Scanner scanner = new QueueingScanner(logger, queue);

		scanner.scan(Paths.get(getClass().getResource("/documents/text/plain.txt").toURI()));
		scanner.scan(Paths.get(getClass().getResource("/documents/ocr/simple.tiff").toURI()));

		consumer.start();
		consumer.awaitTermination();

		Assert.assertEquals("This is a test.\n\nHEAVY\nMETAL\n\n\n", output.toString());
	}

	@Test
	public void testConsumeWithDirectoryScanner() throws Throwable {
		final Extractor extractor = new Extractor(logger);

		final ByteArrayOutputStream output = new ByteArrayOutputStream();
		final PrintStream print = new PrintStream(output);
		final Spewer spewer = new PrintStreamSpewer(logger, print);

		final int threads = 2;
		final BlockingQueue<String> queue = new ArrayBlockingQueue<String>(threads * 2);
		final PollingConsumer consumer = new PollingConsumer(logger, queue, spewer, extractor, threads);
		final Scanner scanner = new QueueingScanner(logger, queue);

		scanner.scan(Paths.get(getClass().getResource("/documents/text/").toURI()));

		consumer.start();
		consumer.awaitTermination();

		Assert.assertEquals("This is a test.\n\nThis is a test.\n\n", output.toString());
	}
}
