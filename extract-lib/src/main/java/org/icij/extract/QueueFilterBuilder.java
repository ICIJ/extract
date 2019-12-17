package org.icij.extract;

import org.icij.extract.queue.DocumentQueue;

import java.io.IOException;

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

    public long execute() throws IOException {
        return extractedStream.extractedDocuments().filter(p -> documentQueue.remove(p)).count();
    }

}
