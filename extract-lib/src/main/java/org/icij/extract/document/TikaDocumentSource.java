package org.icij.extract.document;

import org.apache.tika.metadata.Metadata;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.Supplier;

public class TikaDocumentSource implements Supplier<InputStream> {
    protected final Metadata metadata;
    protected final Supplier<InputStream> contentSupplier;

    public TikaDocumentSource(Metadata metadata,  Supplier<InputStream> contentSupplier) {
        this.metadata = metadata;
        this.contentSupplier = contentSupplier;
    }

    public Metadata metadata() {
        return metadata;
    }

    @Override
    public InputStream get() {
        return contentSupplier.get();
    }

    public byte[] getContent() throws IOException {
        try (InputStream contentStream = get()) {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            int nbTmpBytesRead;
            for (byte[] tmp = new byte[8192]; (nbTmpBytesRead = contentStream.read(tmp)) > 0; ) {
                buffer.write(tmp, 0, nbTmpBytesRead);
            }
            return buffer.toByteArray();
        }
    }
}
