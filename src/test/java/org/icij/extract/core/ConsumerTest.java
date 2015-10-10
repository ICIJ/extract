package org.icij.extract.core;

import org.icij.extract.test.*;
import org.icij.extract.redis.Redis;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import java.nio.file.Path;
import java.nio.file.Paths;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;

import org.redisson.Redisson;
import org.redisson.client.RedisConnectionException;

import org.junit.Test;
import org.junit.Assert;
import org.junit.Assume;

public class ConsumerTest extends TestBase {

	@Test
	public void testConsume() throws Throwable {
		final Extractor extractor = new Extractor(logger);

		final ByteArrayOutputStream output = new ByteArrayOutputStream();
		final PrintStream print = new PrintStream(output);
		final Spewer spewer = new PrintStreamSpewer(logger, print);

		final int threads = 2;
		final Consumer consumer = new Consumer(logger, spewer, extractor, threads);

		final Path file = Paths.get(getClass().getResource("/documents/text/plain.txt").toURI());

		consumer.accept(file);
		consumer.finish();

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
		consumer.drain();
		consumer.finish();

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
			final Redisson redisson = Redis.createClient();
			final BlockingQueue<String> queue = Redis.getBlockingQueue(redisson, "extract:test");
			final PollingConsumer consumer = new PollingConsumer(logger, queue, spewer, extractor, threads);

			final Path file = Paths.get(getClass().getResource("/documents/text/plain.txt").toURI());

			queue.put(file.toString());
			consumer.drain();
			consumer.finish();

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
		final Scanner scanner = new Scanner(logger, queue);

		scanner.scan(Paths.get(getClass().getResource("/documents/text/plain.txt").toURI()));
		scanner.scan(Paths.get(getClass().getResource("/documents/ocr/simple.tiff").toURI()));

		// Block until every single path has been scanned and queued.
		scanner.finish();

		consumer.drain();
		consumer.finish();

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
		final Scanner scanner = new Scanner(logger, queue);

		scanner.scan(Paths.get(getClass().getResource("/documents/text/").toURI()));

		// Block until every single path has been scanned and queued.
		scanner.finish();

		consumer.drain();
		consumer.finish();

		Assert.assertEquals("This is a test.\n\nThis is a test.\n\n", output.toString());
	}
}
