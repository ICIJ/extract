package org.icij.extract.core;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.Assert;

public class ExtractingConsumerTest {

	private Path getFile() throws URISyntaxException {
		return Paths.get(getClass().getResource("/documents/text/plain.txt").toURI());
	}

	@Test
	public void testConsume() throws Throwable {
		final Extractor extractor = new Extractor();

		final ByteArrayOutputStream output = new ByteArrayOutputStream();
		final PrintStream print = new PrintStream(output);
		final Spewer spewer = new PrintStreamSpewer(print);

		final ExtractingConsumer consumer = new ExtractingConsumer(spewer, extractor);

		final Path file = getFile();

		consumer.accept(file);
		consumer.shutdown();
		Assert.assertTrue(consumer.awaitTermination(1, TimeUnit.MINUTES));

		Assert.assertEquals("This is a test.\n\n", output.toString());
	}

	@Test
	public void testSetReporter() throws Throwable {
		final Spewer spewer = new PrintStreamSpewer(new PrintStream(new ByteArrayOutputStream()));
		final ExtractingConsumer consumer = new ExtractingConsumer(spewer, new Extractor());
		final Reporter reporter = new Reporter(new HashMapReport());
		final Path file = getFile();

		// Assert that no reporter is set by default.
		Assert.assertNull(consumer.getReporter());
		consumer.setReporter(reporter);
		Assert.assertEquals(reporter, consumer.getReporter());

		// Assert that the extraction result is reported.
		Assert.assertNull(reporter.result(file));
		consumer.accept(file);
		consumer.shutdown();
		Assert.assertTrue(consumer.awaitTermination(1, TimeUnit.MINUTES));
		Assert.assertEquals(ExtractionResult.SUCCEEDED, reporter.result(file));
	}
}
