package org.icij.extract.report;

import org.icij.extract.document.Document;
import org.icij.extract.extractor.ExtractionStatus;

import java.util.concurrent.ConcurrentMap;

/**
 * The interface for a report.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public interface Report extends ConcurrentMap<Document, ExtractionStatus>, AutoCloseable {

}
