package org.icij.extract.core;

import java.io.Closeable;
import java.nio.file.Path;

import java.util.concurrent.BlockingQueue;
import java.util.function.Supplier;

/**
 * The interface for a queue.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public interface Queue extends BlockingQueue<Path>, Supplier<Path>, Closeable {

	@Override
	default public Path get() {
		return poll();
	}
}
