package org.icij.extract.queue;

import java.util.Set;

public interface DocumentSet<T> extends Set<T>, AutoCloseable {
    String getName();
}
