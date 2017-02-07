package org.icij.extract.report;

import org.icij.extract.document.Document;
import org.icij.extract.extractor.ExtractionStatus;

import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

/**
 * Records the extraction result of a file to the given {@link Report}.
 *
 * @since 1.0.0-beta
 */
public class Reporter implements AutoCloseable {

	private Set<Class<? extends Exception>> journalableTypes = new HashSet<>();
	private Map<Document, ExtractionStatus> journal = new ConcurrentHashMap<>();
	private Semaphore flushing = new Semaphore(1);

	/**
	 * The report to save results to or check.
	 */
	protected final Report report;

	/**
	 * Create a new reporter that will record results to the given {@link Report}.
	 *
	 * @param report the report map
	 */
	public Reporter(final Report report) {
		this.report = report;

		final Collection<Class<? extends Exception>> journalableExceptions = report.journalableExceptions();

		if (null != journalableExceptions) {
			journalableExceptions.forEach(this::journalableException);
		}
	}

	/**
	 * Check the extraction result of a given document.
	 *
	 * @param document the document to check
	 * @return The extraction result or {@code null} if no result was recorded for the file.
	 */
	public ExtractionStatus result(final Document document) {
		return report.get(document);
	}

	/**
	 * Save the extraction for the given document.
	 *
	 * @param document the document
	 * @param result the extraction result
	 */
	public void save(final Document document, final ExtractionStatus result) {
		try {
			report.fastPut(document, result);
		} catch (Exception e) {
			if (journalableTypes.contains(e.getClass())) {
				journal.put(document, result);
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
	 * Check an extraction result.
	 *
	 * @param document the document to check
	 * @param result matched against the actual result
	 * @return {@code true} if the results match or {@code false} if there is no match or no recorded result.
	 */
	public boolean check(final Document document, final ExtractionStatus result) {
		final ExtractionStatus status = result(document);

		return null != status && status.equals(result);
	}

	/**
	 * Check whether a path should be skipped.
	 *
	 * @param document the document to check
	 * @return {@code true} if the document should be skipped.
	 */
	public boolean skip(final Document document) {
		return check(document, ExtractionStatus.SUCCEEDED);
	}

	@Override
	public void close() throws Exception {
		flushing.acquire();

		try {
			flushJournal();
		} finally {
			flushing.release();
		}

		report.close();
	}

	/**
	 * Add a class of {@link Exception} that when caught during {@link #save(Document, ExtractionStatus)}, would add
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
		final Iterator<Map.Entry<Document, ExtractionStatus>> iterator = journal.entrySet().iterator();

		while (iterator.hasNext()) {
			final Map.Entry<Document, ExtractionStatus> entry = iterator.next();

			report.fastPut(entry.getKey(), entry.getValue());
			iterator.remove();
		}
	}
}
