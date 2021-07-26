package org.icij.extract.report;

import org.icij.extract.extractor.ExtractionStatus;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

/**
 * Records the extraction result of a file to the given {@link ReportMap}.
 *
 * @since 1.0.0-beta
 */
public class Reporter implements AutoCloseable {

	private Set<Class<? extends Exception>> journalableTypes = new HashSet<>();
	private Map<Path, Report> journal = new ConcurrentHashMap<>();
	private Semaphore flushing = new Semaphore(1);

	/**
	 * The report to save results to or check.
	 */
	private final ReportMap reportMap;

	/**
	 * Create a new reporter that will record results to the given {@link ReportMap}.
	 *
	 * @param reportMap the report map
	 */
	public Reporter(final ReportMap reportMap) {
		this.reportMap = reportMap;

		final Collection<Class<? extends Exception>> journalableExceptions = reportMap.journalableExceptions();

		if (null != journalableExceptions) {
			journalableExceptions.forEach(this::journalableException);
		}
	}

	/**
	 * Check the extraction result of a given tikaDocument.
	 *
	 * @param path the tikaDocument to check
	 * @return The extraction report or {@code null} if no result was recorded for the file.
	 */
	public Report report(final Path path) {
		return reportMap.get(path);
	}

	/**
	 * Save the extraction report for the given tikaDocument.
	 *
	 * @param path the tikaDocument
	 * @param report the extraction report
	 */
	public void save(final Path path, final Report report) {
		try {
			reportMap.fastPut(path, report);
		} catch (Exception e) {
			if (journalableTypes.contains(e.getClass())) {
				journal.put(path, report);
			}

			throw e;
		}

		if (flushing.tryAcquire()) {
			try {
				flushJournal();
			} finally {
				flushing.release();
			}
		}
	}

	/**
	 * Save the extraction status and optional exception for the given tikaDocument.
	 *
	 * @param path the tikaDocument
	 * @param status the extraction status
	 * @param exception any exception caught during extraction
	 */
	public void save(final Path path, final ExtractionStatus status, final Exception exception) {
		save(path, new Report(status, exception));
	}

	/**
	 * Save the extraction status for the given tikaDocument.
	 *
	 * @param path the tikaDocument
	 * @param status the extraction status
	 */
	public void save(final Path path, final ExtractionStatus status) {
		save(path, new Report(status));
	}

	/**
	 * Check an extraction result.
	 *
	 * @param path the tikaDocument to check
	 * @param result matched against the actual result
	 * @return {@code true} if the results match or {@code false} if there is no match or no recorded result.
	 */
	public boolean check(final Path path, final ExtractionStatus result) {
		final Report report = report(path);

		return null != report && report.getStatus().equals(result);
	}

	/**
	 * Check whether a path should be skipped.
	 *
	 * @param path the tikaDocument to check
	 * @return {@code true} if the tikaDocument should be skipped.
	 */
	public boolean skip(final Path path) {
		return check(path, ExtractionStatus.SUCCESS);
	}

	@Override
	public void close() throws Exception {
		flushing.acquire();

		try {
			flushJournal();
		} finally {
			flushing.release();
		}

		reportMap.close();
	}

	/**
	 * Add a class of {@link Exception} that when caught during {@link #save(Path, Report)}, would add
	 * the arguments to journal which is flushed when the next operation succeeds.
	 *
	 * @param e the class of exception that is temporary
	 */
	private synchronized void journalableException(final Class<? extends Exception> e) {
		journalableTypes.add(e);
	}

	/**
	 * Flush the journal of failed status to the report.
	 */
	private void flushJournal() {
		final Iterator<Map.Entry<Path, Report>> iterator = journal.entrySet().iterator();

		while (iterator.hasNext()) {
			final Map.Entry<Path, Report> entry = iterator.next();

			reportMap.fastPut(entry.getKey(), entry.getValue());
			iterator.remove();
		}
	}
}
