package org.icij.extract.queue;

import org.icij.extract.document.TikaDocument;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collector;

/**
 * The interface for a queue of {@link TikaDocument} objects.
 *
 * @since 2.0.0
 */
public interface DocumentQueue extends BlockingQueue<TikaDocument>, AutoCloseable {
    DocumentQueue newQueue();
    default boolean remove(Object o, int count) {
        boolean removed = false;
        if (count == 0) {
            boolean hasMaybeMore = true;
            while (hasMaybeMore) {
                hasMaybeMore = remove(o);
                removed = removed | hasMaybeMore;
            }
        } else {
            for (int i = 0; i < count; i++) {
                removed = removed | remove(o);
            }
        }
        return removed;
    }

    default int removeDuplicatePaths() {
        Map<TikaDocument, Integer> documents = new HashMap<>();
        final int initialSize = size();
        forEach(doc -> documents.compute(doc, (k, v) -> (v == null) ? 1 : v+1));
        documents.entrySet().stream().filter((e -> e.getValue()>1)).forEach(e -> remove(e.getKey(), e.getValue() - 1));
        return initialSize - size();
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
