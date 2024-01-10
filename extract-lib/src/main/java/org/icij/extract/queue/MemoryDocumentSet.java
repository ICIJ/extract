package org.icij.extract.queue;

import java.util.LinkedHashSet;

public class MemoryDocumentSet<T> extends LinkedHashSet<T> implements DocumentSet<T> {
    private final String name;

    public MemoryDocumentSet(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override public void close() {}
}
