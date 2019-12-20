package org.icij.extract.queue;

import java.nio.file.Path;
import java.util.Set;

public interface DocumentSet extends Set<Path>, AutoCloseable {
}
