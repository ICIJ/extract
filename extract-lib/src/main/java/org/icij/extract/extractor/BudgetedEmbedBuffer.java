package org.icij.extract.extractor;

import org.apache.tika.io.TemporaryResources;
import org.icij.extract.document.TikaDocument.ReaderGenerator;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Buffers one embedded document's extracted text in memory while a shared, per-extraction
 * byte budget allows, overflowing to a temp file once the budget would be exceeded. Once
 * spilled, the buffer stays on disk. The text is read back lazily during the spew walk.
 *
 * <p>The shared {@link AtomicLong} models the bytes currently resident in memory across all
 * of an extraction's buffers; it gates when spilling begins during the (single-threaded)
 * parse. Resident bytes are released from the counter when they are moved to disk or when
 * the embed is discarded on error.
 */
class BudgetedEmbedBuffer extends OutputStream {

    private final AtomicLong reserved;
    private final long budgetBytes;
    private final TemporaryResources tmp;

    private ByteArrayOutputStream memory = new ByteArrayOutputStream(8192);
    private long memoryReserved = 0;
    private Path file = null;
    private OutputStream fileOut = null;
    private boolean closed = false;

    BudgetedEmbedBuffer(final AtomicLong reserved, final long budgetBytes, final TemporaryResources tmp) {
        this.reserved = reserved;
        this.budgetBytes = budgetBytes;
        this.tmp = tmp;
    }

    @Override
    public synchronized void write(final int b) throws IOException {
        write(new byte[]{(byte) b}, 0, 1);
    }

    @Override
    public synchronized void write(final byte[] b, final int off, final int len) throws IOException {
        if (closed) {
            throw new IOException("Stream closed");
        }
        if (file != null) {
            fileOut.write(b, off, len);
            return;
        }
        if (reserved.get() + len > budgetBytes) {
            spill();
            fileOut.write(b, off, len);
            return;
        }
        memory.write(b, off, len);
        memoryReserved += len;
        reserved.addAndGet(len);
    }

    private void spill() throws IOException {
        file = tmp.createTempFile();
        fileOut = new BufferedOutputStream(Files.newOutputStream(file));
        memory.writeTo(fileOut);
        reserved.addAndGet(-memoryReserved);
        memoryReserved = 0;
        memory = null;
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        if (fileOut != null) {
            fileOut.close();
        }
    }

    /** Release this buffer's resident memory bytes from the shared counter (error path). */
    synchronized void discard() {
        if (memoryReserved > 0) {
            reserved.addAndGet(-memoryReserved);
            memoryReserved = 0;
        }
        memory = null;
    }

    /** A lazy reader over the buffered text, from memory or the temp file. */
    ReaderGenerator readerGenerator() {
        return () -> {
            synchronized (this) {
                if (file != null) {
                    return new InputStreamReader(Files.newInputStream(file), StandardCharsets.UTF_8);
                }
                if (memory == null) {
                    throw new IOException("Embed buffer was discarded; no content to read");
                }
                return new InputStreamReader(new ByteArrayInputStream(memory.toByteArray()), StandardCharsets.UTF_8);
            }
        };
    }

    synchronized boolean isSpilled() {
        return file != null;
    }
}
