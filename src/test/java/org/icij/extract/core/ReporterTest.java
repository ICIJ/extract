package org.icij.extract.core;

import java.nio.file.Paths;

import org.icij.extract.document.Document;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.PathIdentifier;
import org.icij.extract.extractor.ExtractionStatus;
import org.icij.extract.report.HashMapReport;
import org.icij.extract.report.Report;
import org.icij.extract.report.Reporter;
import org.junit.Test;
import org.junit.Assert;

public class ReporterTest {

	private static class ReporterStub extends Reporter {

		private boolean closed = false;

		ReporterStub(final Report report) {
			super(report);
		}

		boolean isClosed() {
			return closed;
		}

		@Override
		public void close() throws Exception {
			closed = true;
			super.close();
		}
	}

	private final DocumentFactory factory = new DocumentFactory().withIdentifier(new PathIdentifier());

	@Test
	public void testSave() throws Throwable {
		final Document a = factory.create(Paths.get("/path/to/a"));
		final Document b = factory.create(Paths.get("/path/to/b"));

		final Report report = new HashMapReport();
		final Reporter reporter = new Reporter(report);

		reporter.save(a, ExtractionStatus.SUCCEEDED);
		Assert.assertTrue(reporter.check(a, ExtractionStatus.SUCCEEDED));
		reporter.save(b, ExtractionStatus.NOT_FOUND);
		Assert.assertTrue(reporter.check(b, ExtractionStatus.NOT_FOUND));
		Assert.assertFalse(reporter.check(b, ExtractionStatus.SUCCEEDED));
	}

	@Test
	public void testCloseClosesReport() throws Throwable {
		final Report report = new HashMapReport();
		final ReporterStub reporter = new ReporterStub(report);

		Assert.assertFalse(reporter.isClosed());
		reporter.close();
		Assert.assertTrue(reporter.isClosed());
	}

	@Test
	public void testReporterIsAutoCloseable() throws Throwable {
		final Report report = new HashMapReport();
		ReporterStub reporter;

		try (final ReporterStub _reporter = new ReporterStub(report)) {
			Assert.assertFalse(_reporter.isClosed());
			reporter = _reporter;
		}

		Assert.assertTrue(reporter.isClosed());
	}
}
