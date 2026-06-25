package com.pff;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.zip.Deflater;

import static org.fest.assertions.Assertions.assertThat;

public class OstCompressedBlockReaderTest {

    // Writes the given bytes to a temp file and returns a read-only handle to it.
    private RandomAccessFile fileOf(byte[] content) throws IOException {
        final File f = File.createTempFile("ost-block-test", ".bin");
        f.deleteOnExit();
        try (FileOutputStream fos = new FileOutputStream(f)) {
            fos.write(content);
        }
        return new RandomAccessFile(f, "r");
    }

    // zlib-compresses data the way OST internal compression stores a block (default zlib, 78 9c header).
    private static byte[] zlib(byte[] data) {
        final Deflater deflater = new Deflater();
        deflater.setInput(data);
        deflater.finish();
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final byte[] buf = new byte[1024];
        while (!deflater.finished()) {
            out.write(buf, 0, deflater.deflate(buf));
        }
        deflater.end();
        return out.toByteArray();
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.US_ASCII);
    }

    private static byte[] concat(byte[]... parts) {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (final byte[] p : parts) {
            out.write(p, 0, p.length);
        }
        return out.toByteArray();
    }

    @Test
    public void recoversASingleRawBlock() throws Exception {
        final byte[] payload = bytes("HELLO RAW BLOCK");
        final RandomAccessFile fh = fileOf(payload);
        final List<OstCompressedBlockReader.Block> blocks =
                List.of(new OstCompressedBlockReader.Block(0, payload.length));

        final Optional<byte[]> result =
                OstCompressedBlockReader.recoverFromBlocks(fh, blocks, false, payload.length);

        assertThat(result.isPresent()).isTrue();
        assertThat(Arrays.equals(result.get(), payload)).isTrue();
    }

    @Test
    public void recoversASingleZlibBlock() throws Exception {
        final byte[] plain = bytes("HELLO COMPRESSED WORLD");
        final byte[] compressed = zlib(plain);
        final RandomAccessFile fh = fileOf(compressed);
        final List<OstCompressedBlockReader.Block> blocks =
                List.of(new OstCompressedBlockReader.Block(0, compressed.length));

        final Optional<byte[]> result =
                OstCompressedBlockReader.recoverFromBlocks(fh, blocks, false, plain.length);

        assertThat(result.isPresent()).isTrue();
        assertThat(Arrays.equals(result.get(), plain)).isTrue();
    }

    @Test
    public void recoversMixedRawThenZlibBlocks() throws Exception {
        // The real OST-2013 shape: leading raw blocks, later blocks zlib-compressed.
        final byte[] raw = bytes("ABCD");
        final byte[] zlibPart = zlib(bytes("EFGH"));
        final byte[] file = concat(raw, zlibPart);
        final RandomAccessFile fh = fileOf(file);
        final List<OstCompressedBlockReader.Block> blocks = List.of(
                new OstCompressedBlockReader.Block(0, raw.length),
                new OstCompressedBlockReader.Block(raw.length, zlibPart.length));

        final Optional<byte[]> result =
                OstCompressedBlockReader.recoverFromBlocks(fh, blocks, false, 8);

        assertThat(result.isPresent()).isTrue();
        assertThat(Arrays.equals(result.get(), bytes("ABCDEFGH"))).isTrue();
    }

    @Test
    public void rejectsWhenReassembledSizeDoesNotMatchDeclared() throws Exception {
        final byte[] payload = bytes("EIGHTBYT");
        final RandomAccessFile fh = fileOf(payload);
        final List<OstCompressedBlockReader.Block> blocks =
                List.of(new OstCompressedBlockReader.Block(0, payload.length));

        final Optional<byte[]> result =
                OstCompressedBlockReader.recoverFromBlocks(fh, blocks, false, 999);

        assertThat(result.isPresent()).isFalse();
    }

    @Test
    public void fallsBackToRawWhenZlibHeaderDoesNotInflate() throws Exception {
        // 0x78 0x9C is a valid zlib header (0x789C % 31 == 0) but this body is not deflate data,
        // so inflation must fail and the raw bytes must be used instead.
        final byte[] payload = new byte[]{(byte) 0x78, (byte) 0x9C, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF};
        final RandomAccessFile fh = fileOf(payload);
        final List<OstCompressedBlockReader.Block> blocks =
                List.of(new OstCompressedBlockReader.Block(0, payload.length));

        final Optional<byte[]> result =
                OstCompressedBlockReader.recoverFromBlocks(fh, blocks, false, payload.length);

        assertThat(result.isPresent()).isTrue();
        assertThat(Arrays.equals(result.get(), payload)).isTrue();
    }

    @Test
    public void decryptsACompressibleEncryptedBlock() throws Exception {
        // On disk the bytes are permuted; PSTObject.encode produces what decode() reverses.
        final byte[] plain = bytes("SECRET PAYLOAD");
        final byte[] onDisk = PSTObject.encode(Arrays.copyOf(plain, plain.length));
        final RandomAccessFile fh = fileOf(onDisk);
        final List<OstCompressedBlockReader.Block> blocks =
                List.of(new OstCompressedBlockReader.Block(0, onDisk.length));

        final Optional<byte[]> result =
                OstCompressedBlockReader.recoverFromBlocks(fh, blocks, true, plain.length);

        assertThat(result.isPresent()).isTrue();
        assertThat(Arrays.equals(result.get(), plain)).isTrue();
    }
}
