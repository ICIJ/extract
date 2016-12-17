package org.icij.extract.report;

import org.icij.extract.document.Document;
import org.icij.extract.extractor.ExtractionStatus;

import java.util.concurrent.ConcurrentHashMap;

/**
 * A {@link Report} using a {@link ConcurrentHashMap} as a backend.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public class HashMapReport extends ConcurrentHashMap<Document, ExtractionStatus> implements Report {

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
