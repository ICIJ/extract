package org.icij.extract.queue;

import org.icij.extract.document.TikaDocument;

import java.util.concurrent.BlockingQueue;

/**
 * The interface for a queue of {@link TikaDocument} objects.
 *
 * @since 2.0.0
 */
public interface DocumentQueue extends BlockingQueue<TikaDocument>, AutoCloseable {
    DocumentQueue newQueue();
}
