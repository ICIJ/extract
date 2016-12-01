package org.icij.extract.queue;

import java.nio.file.Path;

import java.util.concurrent.BlockingQueue;

/**
 * The interface for a queue of {@link Path} objects.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public interface PathQueue extends BlockingQueue<Path>, AutoCloseable {

}
