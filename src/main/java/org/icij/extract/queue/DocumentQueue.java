package org.icij.extract.queue;

import org.icij.extract.document.Document;

import java.util.concurrent.BlockingQueue;

/**
 * The interface for a queue of {@link Document} objects.
 *
 * @since 2.0.0
 */
public interface DocumentQueue extends BlockingQueue<Document>, AutoCloseable {

}
