package org.icij.extract.core;

import java.io.Closeable;
import java.nio.file.Path;

import java.util.concurrent.ConcurrentMap;

/**
 * The interface for a report.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public interface Report extends ConcurrentMap<Path, ReportResult>, Closeable {

}
