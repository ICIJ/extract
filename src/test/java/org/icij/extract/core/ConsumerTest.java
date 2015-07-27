package org.icij.extract.core;

import org.icij.extract.test.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import java.nio.file.Path;
import java.nio.file.Paths;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;

import org.redisson.Redisson;
import com.lambdaworks.redis.RedisConnectionException;

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

		consumer.consume(file);
		consumer.shutdown();
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
		consumer.drain();
		consumer.shutdown();
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
			consumer.drain();
			consumer.shutdown();
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
		final QueueingScanner scanner = new QueueingScanner(logger, queue);

		scanner.scan(Paths.get(getClass().getResource("/documents/text/plain.txt").toURI()));
		scanner.scan(Paths.get(getClass().getResource("/documents/ocr/simple.tiff").toURI()));

		// Block until every single path has been scanned and queued.
		scanner.shutdown();
		scanner.awaitTermination();

		consumer.drain();
		consumer.shutdown();
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
		final QueueingScanner scanner = new QueueingScanner(logger, queue);

		scanner.scan(Paths.get(getClass().getResource("/documents/text/").toURI()));

		// Block until every single path has been scanned and queued.
		scanner.shutdown();
		scanner.awaitTermination();

		consumer.drain();
		consumer.shutdown();
		consumer.awaitTermination();

		Assert.assertEquals("This is a test.\n\nThis is a test.\n\n", output.toString());
	}
}
