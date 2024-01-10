package org.icij.extract.queue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

/**
 * The interface for a document queue represented by {@link T} objects.
 * T should provide a unique attribute to identify Documents
 * @since 2.0.0
 */
public interface DocumentQueue<T> extends BlockingQueue<T>, AutoCloseable {
    String getName();
    boolean delete();

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
        Map<T, Integer> documents = new HashMap<>();
        final int initialSize = size();
        forEach(path -> documents.compute(path, (k, v) -> (v == null) ? 1 : v+1));
        documents.entrySet().stream().filter((e -> e.getValue()>1)).forEach(e -> remove(e.getKey(), e.getValue() - 1));
        return initialSize - size();
    }
}
