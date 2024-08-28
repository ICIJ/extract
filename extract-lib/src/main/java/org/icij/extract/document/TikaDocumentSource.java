package org.icij.extract.document;

import org.apache.tika.metadata.Metadata;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public record TikaDocumentSource(Metadata metadata, File content) {
    public byte[] getContent() throws IOException {
        try (InputStream contentStream = new FileInputStream(content)) {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            int nbTmpBytesRead;
            for (byte[] tmp = new byte[8192]; (nbTmpBytesRead = contentStream.read(tmp)) > 0; ) {
                buffer.write(tmp, 0, nbTmpBytesRead);
            }
            return buffer.toByteArray();
        }
    }
}
