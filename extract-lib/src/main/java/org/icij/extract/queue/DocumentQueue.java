package org.icij.extract.queue;

import org.icij.extract.document.TikaDocument;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

/**
 * The interface for a queue of {@link TikaDocument} objects.
 *
 * @since 2.0.0
 */
public interface DocumentQueue extends BlockingQueue<Path>, AutoCloseable {
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

    default int removeDuplicates() {
        Map<Path, Integer> documents = new HashMap<>();
        final int initialSize = size();
        forEach(path -> documents.compute(path, (k, v) -> (v == null) ? 1 : v+1));
        documents.entrySet().stream().filter((e -> e.getValue()>1)).forEach(e -> remove(e.getKey(), e.getValue() - 1));
        return initialSize - size();
    }
}
