package org.icij.extract.extractor;

import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.PathIdentifier;
import org.icij.extract.report.HashMapReportMap;
import org.icij.extract.report.Reporter;
import org.icij.spewer.FieldNames;
import org.icij.spewer.PrintStreamSpewer;
import org.icij.spewer.Spewer;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

public class TikaDocumentConsumerTest {

	private final DocumentFactory factory = new DocumentFactory().withIdentifier(new PathIdentifier());

	private Path getFile() throws URISyntaxException {
		return Paths.get(getClass().getResource("/documents/text/plain.txt").toURI());
	}

	@Test
	public void testConsume() throws Throwable {
		final Extractor extractor = new Extractor();

		final ByteArrayOutputStream output = new ByteArrayOutputStream();
		final PrintStream print = new PrintStream(output);
		final Spewer spewer = new PrintStreamSpewer(print, new FieldNames());
		final DocumentConsumer consumer = new DocumentConsumer(spewer, extractor, 1);
		final Path tikaDocument = getFile();

		spewer.outputMetadata(false);
		consumer.accept(tikaDocument);
		consumer.shutdown();
		Assert.assertTrue(consumer.awaitTermination(1, TimeUnit.MINUTES));

		Assert.assertEquals("This is a test.", output.toString().trim());
	}

	@Test
	public void testSetReporter() throws Throwable {
		final Spewer spewer = new PrintStreamSpewer(new PrintStream(new ByteArrayOutputStream()), new FieldNames());
		final DocumentConsumer consumer = new DocumentConsumer(spewer, new Extractor(), 1);
		final Reporter reporter = new Reporter(new HashMapReportMap());
		final Path tikaDocument = getFile();

		// Assert that no reporter is set by default.
		Assert.assertNull(consumer.getReporter());
		consumer.setReporter(reporter);
		Assert.assertEquals(reporter, consumer.getReporter());

		// Assert that the extraction result is reported.
		Assert.assertNull(reporter.report(tikaDocument));
		spewer.outputMetadata(false);
		consumer.accept(tikaDocument);
		consumer.shutdown();
		Assert.assertTrue(consumer.awaitTermination(1, TimeUnit.MINUTES));
		Assert.assertEquals(ExtractionStatus.SUCCESS, reporter.report(tikaDocument).getStatus());
	}
}
