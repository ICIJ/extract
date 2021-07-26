package org.icij.extract.report;

import java.nio.file.Path;
import java.util.Collection;
import java.util.concurrent.ConcurrentMap;

/**
 * The interface for a report map.
 *
 *
 */
public interface ReportMap extends ConcurrentMap<Path, Report>, AutoCloseable {

	/**
	 *  Allow implementations to define a faster method for putting values into the map that doesn't require the
	 *  previous value to be returned. This can reduce the processing time for kinds of maps and/or reduce the
	 *  RTT (round-trip time) for distributed maps.
	 *
	 * @param key the document to set the status of
	 * @param value the new status
	 * @return true if the field was new when the value was set; false if it was updated
	 */
	boolean fastPut(final Path key, final Report value);

	/**
	 * Allow implementations to define a list of exception classes that when caught, would indicate to the caller
	 * that arguments should be journaled and flushed later.
	 *
	 * @return a collection of exception classes
	 */
	default Collection<Class<? extends Exception>> journalableExceptions() {
		return null;
	}
}
