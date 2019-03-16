package org.icij.extract.queue;

import org.icij.extract.document.TikaDocument;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collector;

/**
 * The interface for a queue of {@link TikaDocument} objects.
 *
 * @since 2.0.0
 */
public interface DocumentQueue extends BlockingQueue<TikaDocument>, AutoCloseable {
    DocumentQueue newQueue();

    default void removeDuplicatePaths() {
        Set<Path> documents = new HashSet<>();
        forEach(doc -> {
            if (documents.contains(doc.getPath())) {remove(doc);}
            else {documents.add(doc.getPath());}
        });
    }

    default Collector<TikaDocument, DocumentQueue, DocumentQueue> toDocumentQueue() {
        return Collector.of(
                this::newQueue,
                BlockingQueue::add,
                (r1, r2) -> {
                    r1.addAll(r2);
                    return r1;
                }
        );
    }
}
