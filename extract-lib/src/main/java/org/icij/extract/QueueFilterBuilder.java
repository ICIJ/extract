package org.icij.extract;

import org.icij.extract.queue.DocumentQueue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

public class QueueFilterBuilder {
    private ExtractedStreamer extractedStream;
    private DocumentQueue documentQueue;

    public QueueFilterBuilder filter(DocumentQueue documentQueue) {
        this.documentQueue = documentQueue;
        return this;
    }

    public QueueFilterBuilder with(ExtractedStreamer extractedStream) {
        this.extractedStream = extractedStream;
        return this;
    }

    public DocumentQueue execute() throws IOException {
        Set<Path> extractedSet = this.extractedStream.extractedDocuments().collect(toSet());
        return documentQueue.stream().filter(d -> !extractedSet.contains(d.getPath())).collect(documentQueue.toDocumentQueue());
    }

}
