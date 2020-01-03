package org.icij.extract.queue;

import java.nio.file.Path;
import java.util.LinkedHashSet;

public class MemoryDocumentSet extends LinkedHashSet<Path> implements DocumentSet {
    private final String name;

    @Override public void close() { this.clear();}

    public MemoryDocumentSet(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }
}
