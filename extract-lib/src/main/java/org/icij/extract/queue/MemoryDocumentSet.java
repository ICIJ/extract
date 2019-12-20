package org.icij.extract.queue;

import java.nio.file.Path;
import java.util.LinkedHashSet;

public class MemoryDocumentSet extends LinkedHashSet<Path> implements DocumentSet {
    @Override public void close() { this.clear();}
}
