package org.icij.extract.report;

import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A {@link ReportMap} using a {@link ConcurrentHashMap} as a backend.
 *
 *
 */
public class HashMapReportMap extends ConcurrentHashMap<Path, Report> implements ReportMap {

	private static final long serialVersionUID = -1686535587329141323L;

	/**
	 * Instantiate a new report with the default {@code ConcurrentHashMap} capacity (16).
	 */
	public HashMapReportMap() {
		super();
	}

	@Override
	public boolean fastPut(final Path key, final Report value) {
		return put(key, value) != null;
	}

	@Override
	public void close() {
		super.clear();
	}
}
