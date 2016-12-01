package org.icij.extract.core;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.icij.extract.extractor.ExtractionResult;
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

	@Test
	public void testSave() throws Throwable {
		final Path a = Paths.get("/path/to/a");
		final Path b = Paths.get("/path/to/b");

		final Report report = new HashMapReport();
		final Reporter reporter = new Reporter(report);

		reporter.save(a, ExtractionResult.SUCCEEDED);
		Assert.assertTrue(reporter.check(a, ExtractionResult.SUCCEEDED));
		reporter.save(b, ExtractionResult.NOT_FOUND);
		Assert.assertTrue(reporter.check(b, ExtractionResult.NOT_FOUND));
		Assert.assertFalse(reporter.check(b, ExtractionResult.SUCCEEDED));
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
