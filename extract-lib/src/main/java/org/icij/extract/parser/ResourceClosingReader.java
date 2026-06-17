package org.icij.extract.parser;

import java.io.Closeable;
import java.io.FilterReader;
import java.io.IOException;
import java.io.Reader;

/**
 * A {@link Reader} that closes an additional {@link Closeable} resource when it is closed.
 * Used to tie the lifetime of an extraction's spilled embed-text temp files to the
 * close() of the root document reader, which is the universal end-of-extraction signal.
 */
public class ResourceClosingReader extends FilterReader {

    private final Closeable resource;

    public ResourceClosingReader(final Reader in, final Closeable resource) {
        super(in);
        this.resource = resource;
    }

    @Override
    public void close() throws IOException {
        try {
            super.close();
        } finally {
            resource.close();
        }
    }
}
