package org.icij.extract.report;

import org.icij.extract.document.Document;
import org.icij.extract.extractor.ExtractionStatus;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;

/**
 * The interface for a report.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public interface Report extends ConcurrentMap<Document, ExtractionStatus>, AutoCloseable {

	/**
	 *  Allow implementations to define a faster method for putting values into the map that doesn't require the
	 *  previous value to be returned. This can reduce the processing time for kinds of maps and/or reduce the
	 *  RTT (round-trip time) for distributed maps.
	 *
	 * @param key the document to set the status of
	 * @param value the new status
	 * @return true if the field was new when the value was set; false if it was updated
	 */
	default boolean fastPut(final Document key, final ExtractionStatus value) {
		return put(key, value) == null;
	}

	/**
	 * Allow implementations to define a list of exception classes that when caught, would indicate to the caller
	 * that arguments should be journaled and flushed later.
	 *
	 * @return a collection of exception classes
	 */
	default Optional<Collection<Class<? extends Exception>>> journalableExceptions() {
		return Optional.empty();
	}
}
