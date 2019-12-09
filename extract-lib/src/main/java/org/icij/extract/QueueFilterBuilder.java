package org.icij.extract;

import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.PathIdentifier;
import org.icij.extract.queue.DocumentQueue;

import java.io.IOException;

public class QueueFilterBuilder {
    private ExtractedStreamer extractedStream;
    private DocumentQueue documentQueue;
    private DocumentFactory documentFactory = new DocumentFactory().withIdentifier(new PathIdentifier());

    public QueueFilterBuilder filter(DocumentQueue documentQueue) {
        this.documentQueue = documentQueue;
        return this;
    }

    public QueueFilterBuilder with(ExtractedStreamer extractedStream) {
        this.extractedStream = extractedStream;
        return this;
    }

    public QueueFilterBuilder with(DocumentFactory factory) {
        this.documentFactory = factory;
        return this;
    }

    public long execute() throws IOException {
        return extractedStream.extractedDocuments().filter(p -> documentQueue.remove(documentFactory.create(p))).count();
    }

}
