package org.icij.extract.extractor;

import org.icij.extract.report.HashMapReportMap;
import org.icij.extract.report.Report;
import org.icij.extract.report.Reporter;
import org.icij.spewer.PrintStreamSpewer;
import org.icij.spewer.Spewer;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.fest.assertions.Assertions.assertThat;

public class ExtractorErrorBoundaryTest {

	/** An Extractor whose parsing step throws a chosen throwable. */
	private static class ThrowingExtractor extends Extractor {
		private final Throwable toThrow;

		ThrowingExtractor(final Throwable toThrow) {
			this.toThrow = toThrow;
		}

		@Override
		public void extract(final Path path, final Spewer spewer) throws IOException {
			if (toThrow instanceof IOException) {
				throw (IOException) toThrow;
			}
			if (toThrow instanceof RuntimeException) {
				throw (RuntimeException) toThrow;
			}
			if (toThrow instanceof Error) {
				throw (Error) toThrow;
			}
			throw new IllegalStateException("unexpected throwable in test", toThrow);
		}
	}

	private Spewer nullSpewer() {
		return new PrintStreamSpewer(new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8),
				new org.icij.spewer.FieldNames());
	}

	@Test
	public void testRecoverableErrorRecordsFailureFatalAndDoesNotThrow() {
		final HashMapReportMap reportMap = new HashMapReportMap();
		final Reporter reporter = new Reporter(reportMap);
		final Path path = Paths.get("recoverable");

		new ThrowingExtractor(new StackOverflowError()).extract(path, nullSpewer(), reporter);

		final Report report = reportMap.get(path);
		assertThat(report).isNotNull();
		assertThat(report.getStatus()).isEqualTo(ExtractionStatus.FAILURE_FATAL);
	}

	@Test
	public void testFatalErrorRecordsFailureFatalThenRethrows() {
		final HashMapReportMap reportMap = new HashMapReportMap();
		final Reporter reporter = new Reporter(reportMap);
		final Path path = Paths.get("fatal");
		final OutOfMemoryError oom = new OutOfMemoryError("synthetic");

		Throwable caught = null;
		try {
			new ThrowingExtractor(oom).extract(path, nullSpewer(), reporter);
		} catch (final Throwable t) {
			caught = t;
		}

		assertThat(caught).isSameAs(oom);
		final Report report = reportMap.get(path);
		assertThat(report).isNotNull();
		assertThat(report.getStatus()).isEqualTo(ExtractionStatus.FAILURE_FATAL);
	}

	@Test
	public void testRecordingFailureDoesNotMaskFatalError() {
		final OutOfMemoryError oom = new OutOfMemoryError("synthetic");
		// A reporter whose save throws — simulates inability to record under memory pressure.
		final Reporter reporter = new Reporter(new HashMapReportMap()) {
			@Override
			public void save(final Path path, final ExtractionStatus status, final Exception exception) {
				throw new RuntimeException("cannot record");
			}
		};

		Throwable caught = null;
		try {
			new ThrowingExtractor(oom).extract(Paths.get("fatal"), nullSpewer(), reporter);
		} catch (final Throwable t) {
			caught = t;
		}

		assertThat(caught).isSameAs(oom);
	}

	@Test
	public void testCheckedExceptionStillRecordsNonFatalStatus() {
		final HashMapReportMap reportMap = new HashMapReportMap();
		final Reporter reporter = new Reporter(reportMap);
		final Path path = Paths.get("checked");

		new ThrowingExtractor(new IOException("boom")).extract(path, nullSpewer(), reporter);

		final Report report = reportMap.get(path);
		assertThat(report).isNotNull();
		assertThat(report.getStatus()).isEqualTo(ExtractionStatus.FAILURE_UNREADABLE);
	}
}
