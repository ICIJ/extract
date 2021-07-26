package org.icij.extract.document;

import org.apache.tika.metadata.Metadata;

public class TikaDocumentSource {
    public final Metadata metadata;
    public final byte[] content;

    public TikaDocumentSource(final Metadata metadata, final byte[] content) {
        this.metadata = metadata;
        this.content = content;
    }
}
