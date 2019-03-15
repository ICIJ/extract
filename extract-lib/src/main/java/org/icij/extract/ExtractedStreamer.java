package org.icij.extract;

import java.nio.file.Path;
import java.util.stream.Stream;

public interface ExtractedStreamer {
    Stream<Path> extractedDocuments();
}
