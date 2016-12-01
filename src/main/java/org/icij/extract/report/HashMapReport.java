package org.icij.extract.core;

import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A {@link Report} using a {@link ConcurrentHashMap} as a backend.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public class HashMapReport extends ConcurrentHashMap<Path, ExtractionResult> implements Report {

	private static final long serialVersionUID = -1686535587329141323L;

	/**
	 * Instantiate a new report with the default {@code ConcurrentHashMap} capacity (16).
	 */
	public HashMapReport() {
		super();
	}

	@Override
	public void close() {
		super.clear();
	}
}
