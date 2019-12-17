package org.icij.extract.report;

import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.PathIdentifier;
import org.icij.extract.extractor.ExtractionStatus;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

public class ReporterTest {

	private static class ReporterStub extends Reporter {

		private boolean closed = false;

		ReporterStub(final ReportMap reportMap) {
			super(reportMap);
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
	public void testSave() {
		final Path a = Paths.get("/path/to/a");
		final Path b = Paths.get("/path/to/b");

		final ReportMap reportMap = new HashMapReportMap();
		final Reporter reporter = new Reporter(reportMap);

		reporter.save(a, ExtractionStatus.SUCCESS);
		Assert.assertTrue(reporter.check(a, ExtractionStatus.SUCCESS));
		reporter.save(b, ExtractionStatus.FAILURE_NOT_FOUND);
		Assert.assertTrue(reporter.check(b, ExtractionStatus.FAILURE_NOT_FOUND));
		Assert.assertFalse(reporter.check(b, ExtractionStatus.SUCCESS));
	}

	@Test
	public void testCloseClosesReport() throws Throwable {
		final ReportMap reportMap = new HashMapReportMap();
		final ReporterStub reporter = new ReporterStub(reportMap);

		Assert.assertFalse(reporter.isClosed());
		reporter.close();
		Assert.assertTrue(reporter.isClosed());
	}

	@Test
	public void testReporterIsAutoCloseable() throws Throwable {
		final ReportMap reportMap = new HashMapReportMap();
		ReporterStub reporter;

		try (final ReporterStub _reporter = new ReporterStub(reportMap)) {
			Assert.assertFalse(_reporter.isClosed());
			reporter = _reporter;
		}

		Assert.assertTrue(reporter.isClosed());
	}
}
