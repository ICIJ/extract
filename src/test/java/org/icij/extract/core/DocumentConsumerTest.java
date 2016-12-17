package org.icij.extract.core;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import org.icij.extract.document.Document;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.PathIdentifier;
import org.icij.extract.extractor.DocumentConsumer;
import org.icij.extract.extractor.ExtractionStatus;
import org.icij.extract.extractor.Extractor;
import org.icij.extract.report.HashMapReport;
import org.icij.extract.report.Reporter;
import org.icij.extract.spewer.FieldNames;
import org.icij.extract.spewer.PrintStreamSpewer;
import org.icij.extract.spewer.Spewer;
import org.junit.Test;
import org.junit.Assert;

public class DocumentConsumerTest {

	private final DocumentFactory factory = new DocumentFactory().withIdentifier(new PathIdentifier());

	private Document getFile() throws URISyntaxException {
		return factory.create(Paths.get(getClass().getResource("/documents/text/plain.txt").toURI()));
	}

	@Test
	public void testConsume() throws Throwable {
		final Extractor extractor = new Extractor();

		final ByteArrayOutputStream output = new ByteArrayOutputStream();
		final PrintStream print = new PrintStream(output);
		final Spewer spewer = new PrintStreamSpewer(print, new FieldNames());

		final DocumentConsumer consumer = new DocumentConsumer(spewer, extractor, 1);

		final Document document = getFile();

		consumer.accept(document);
		consumer.shutdown();
		Assert.assertTrue(consumer.awaitTermination(1, TimeUnit.MINUTES));

		Assert.assertEquals("This is a test.\n\n", output.toString());
	}

	@Test
	public void testSetReporter() throws Throwable {
		final Spewer spewer = new PrintStreamSpewer(new PrintStream(new ByteArrayOutputStream()), new FieldNames());
		final DocumentConsumer consumer = new DocumentConsumer(spewer, new Extractor(), 1);
		final Reporter reporter = new Reporter(new HashMapReport());
		final Document document = getFile();

		// Assert that no reporter is set by default.
		Assert.assertNull(consumer.getReporter());
		consumer.setReporter(reporter);
		Assert.assertEquals(reporter, consumer.getReporter());

		// Assert that the extraction result is reported.
		Assert.assertNull(reporter.result(document));
		consumer.accept(document);
		consumer.shutdown();
		Assert.assertTrue(consumer.awaitTermination(1, TimeUnit.MINUTES));
		Assert.assertEquals(ExtractionStatus.SUCCEEDED, reporter.result(document));
	}
}
