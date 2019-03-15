package org.icij.extract;

import org.icij.extract.document.TikaDocument;
import org.icij.extract.queue.DocumentQueue;

import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collector;

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

    public DocumentQueue execute() {
        Set<Path> extractedSet = this.extractedStream.extractedDocuments().collect(toSet());
        return documentQueue.stream().filter(d -> !extractedSet.contains(d.getPath())).collect(toDocumentQueue());
    }

    private Collector<TikaDocument, DocumentQueue, DocumentQueue> toDocumentQueue() {
        return Collector.of(
                () -> documentQueue.newQueue(),
                BlockingQueue::add,
                (r1, r2) -> {
                    r1.addAll(r2);
                    return r1;
                }
        );
    }
}
