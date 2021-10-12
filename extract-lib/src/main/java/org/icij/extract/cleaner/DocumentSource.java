package org.icij.extract.cleaner;

import java.io.ByteArrayOutputStream;

public class DocumentSource {
    private final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    public byte[] getContent() {
        return outputStream.toByteArray();
    }

    public ByteArrayOutputStream getOutputStream() {
        return outputStream;
    }
}
