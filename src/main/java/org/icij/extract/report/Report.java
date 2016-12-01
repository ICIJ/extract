package org.icij.extract.report;

import org.icij.extract.extractor.ExtractionResult;

import java.nio.file.Path;

import java.util.concurrent.ConcurrentMap;

/**
 * The interface for a report.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public interface Report extends ConcurrentMap<Path, ExtractionResult>, AutoCloseable {

}
