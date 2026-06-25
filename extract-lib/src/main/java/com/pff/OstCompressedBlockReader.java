package com.pff;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

/**
 * Pure-JVM recovery reader for OST-2013 ("64-bit, 4k page", type-36) by-value attachments that
 * java-libpst 0.9.3 drops.
 *
 * <p>java-libpst resolves an attachment's data blocks correctly but only inflates the per-block zlib
 * "internal compression" in two narrow cases (every block compressed, or one zlib stream spanning all
 * blocks). The real OST-2013 layout is mixed — some blocks stored raw, later blocks zlib-compressed —
 * which it never inflates, so it returns corrupt bytes or throws. This reader re-resolves the same
 * blocks and inflates each one independently when it carries a zlib header, reproducing libpff's bytes
 * with zero native dependency.
 *
 * <p>Lives in {@code com.pff} on purpose: it needs package-private access to java-libpst internals
 * ({@code OffsetIndexItem.fileOffset/size}, {@code PSTFile.getOffsetIndexNode}, {@code PSTObject.decode})
 * and the frozen library is consumed from the classpath, so a split package is safe here. This is classpath-only: a split package is illegal under the JPMS module path, so this seam would need revisiting if extract-lib were ever deployed as a named module.
 */
public final class OstCompressedBlockReader {

    private OstCompressedBlockReader() {
    }

    /** A resolved leaf data block: where it lives in the file and how many bytes it occupies. */
    static final class Block {
        final long fileOffset;
        final int size;

        Block(final long fileOffset, final int size) {
            this.fileOffset = fileOffset;
            this.size = size;
        }
    }

    // PidTagAttachDataBinary — the property id holding a by-value attachment's bytes.
    private static final int PID_TAG_ATTACH_DATA_BINARY = 0x3701;
    // PSTFile encryption type for compressible (permute) encryption; java-libpst decodes only this one.
    private static final int ENCRYPTION_TYPE_COMPRESSIBLE = 1;

    /**
     * Recovers a by-value attachment's bytes that java-libpst could not read. Best-effort: returns
     * empty on any failure (inline data, missing descriptor, I/O error, size-gate mismatch) and never
     * throws, so a single bad attachment never aborts the surrounding PST walk.
     */
    public static Optional<byte[]> recover(final PSTAttachment attachment) {
        if (attachment == null) {
            return Optional.empty();
        }
        try {
            final PSTTableBCItem dataItem = attachment.items.get(PID_TAG_ATTACH_DATA_BINARY);
            if (dataItem == null || !dataItem.isExternalValueReference) {
                // Inline (small) attachment: not the multi-block failure class; nothing extra to recover.
                return Optional.empty();
            }
            final PSTFile pstFile = attachment.pstFile;
            final PSTDescriptorItem descriptor = attachment.localDescriptorItems == null
                    ? null
                    : attachment.localDescriptorItems.get(dataItem.entryValueReference);
            if (descriptor == null) {
                return Optional.empty();
            }
            final long declaredSize = descriptor.getDataSize();
            final OffsetIndexItem root = pstFile.getOffsetIndexNode(descriptor.offsetIndexIdentifier);
            final List<Block> blocks = resolveLeafBlocks(pstFile, root);
            final boolean encrypted = pstFile.getEncryptionType() == ENCRYPTION_TYPE_COMPRESSIBLE;
            return recoverFromBlocks(pstFile.getFileHandle(), blocks, encrypted, declaredSize);
        } catch (final Exception | LinkageError e) {
            return Optional.empty();
        }
    }

    /**
     * Reads and reassembles the given blocks into the attachment's original bytes.
     *
     * @param fileHandle  the open OST file
     * @param blocks      the resolved leaf data blocks, in order
     * @param encrypted   true when the OST uses compressible (permute) encryption
     * @param declaredSize the attachment's declared (uncompressed) size; the integrity gate
     * @return the recovered bytes when their length equals {@code declaredSize}, else empty
     */
    static Optional<byte[]> recoverFromBlocks(final RandomAccessFile fileHandle, final List<Block> blocks,
                                              final boolean encrypted, final long declaredSize) throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream(
                declaredSize > 0 && declaredSize < Integer.MAX_VALUE ? (int) declaredSize : 8192);
        for (final Block block : blocks) {
            final byte[] raw = readExtent(fileHandle, block.fileOffset, block.size);
            if (encrypted) {
                PSTObject.decode(raw); // in-place compressible-encryption (permute) reversal
            }
            final byte[] inflated = looksLikeZlib(raw) ? tryInflate(raw) : null;
            if (inflated != null) {
                out.write(inflated, 0, inflated.length);
            } else {
                out.write(raw, 0, raw.length);
            }
        }
        final byte[] result = out.toByteArray();
        if (declaredSize <= 0 || result.length != declaredSize) {
            return Optional.empty();
        }
        return Optional.of(result);
    }

    // True when the first two bytes are a structurally valid zlib header: deflate method (CM=8) and
    // the (CMF,FLG) pair a multiple of 31. Cheap pre-filter; tryInflate is the real test.
    private static boolean looksLikeZlib(final byte[] data) {
        if (data.length < 2) {
            return false;
        }
        final int cmf = data[0] & 0xFF;
        final int flg = data[1] & 0xFF;
        if ((cmf & 0x0F) != 8) {
            return false;
        }
        return ((cmf << 8) | flg) % 31 == 0;
    }

    // Inflates a complete zlib stream. Returns null when the bytes are not actually a complete,
    // dictionary-free zlib block, so the caller can fall back to treating them as raw.
    private static byte[] tryInflate(final byte[] data) {
        final Inflater inflater = new Inflater();
        inflater.setInput(data);
        final ByteArrayOutputStream out = new ByteArrayOutputStream(Math.max(64, data.length * 3));
        final byte[] buf = new byte[8192];
        try {
            while (!inflater.finished() && !inflater.needsInput() && !inflater.needsDictionary()) {
                final int n = inflater.inflate(buf);
                if (n > 0) {
                    out.write(buf, 0, n);
                }
            }
            return inflater.finished() ? out.toByteArray() : null;
        } catch (final DataFormatException e) {
            return null;
        } finally {
            inflater.end();
        }
    }

    /**
     * Resolves the ordered leaf data blocks for an attachment's data node, walking any XBlock/XXBlock
     * tree the way java-libpst does. Structural blocks are read raw; only the returned leaf blocks
     * carry attachment data.
     */
    static List<Block> resolveLeafBlocks(final PSTFile pstFile, final OffsetIndexItem root)
            throws IOException, PSTException {
        final List<Block> leaves = new ArrayList<>();
        collectLeaves(pstFile, root, leaves);
        return leaves;
    }

    private static void collectLeaves(final PSTFile pstFile, final OffsetIndexItem item, final List<Block> leaves)
            throws IOException, PSTException {
        final boolean internal = (item.indexIdentifier & 0x02L) != 0L;
        final byte[] data = readRaw(pstFile, item.fileOffset, item.size);
        if (internal && item.size >= 8 && data[0] == 0x01) {
            walkXBlock(pstFile, data, leaves);
        } else {
            leaves.add(new Block(item.fileOffset, item.size));
        }
    }

    private static void walkXBlock(final PSTFile pstFile, final byte[] data, final List<Block> leaves)
            throws IOException, PSTException {
        // A corrupt or short XBlock must fail cleanly (recover() catches IOException and counts the
        // attachment unrecovered) rather than throwing AIOOBE from the entry-decode loop.
        if (data.length < 8) {
            throw new IOException("OST XBlock too short: " + data.length + " bytes");
        }
        final int numberOfEntries = (int) PSTObject.convertLittleEndianBytesToLong(data, 2, 4);
        final int arraySize = pstFile.getPSTFileType() == PSTFile.PST_TYPE_ANSI ? 4 : 8;
        if (numberOfEntries < 0 || 8L + (long) numberOfEntries * arraySize > data.length) {
            throw new IOException("OST XBlock entry table out of bounds: entries=" + numberOfEntries
                    + " arraySize=" + arraySize + " dataLength=" + data.length);
        }
        int offset = 8;
        if (data[1] == 0x02) { // XXBlock: entries point to XBlocks
            for (int x = 0; x < numberOfEntries; x++) {
                final long bid =
                        PSTObject.convertLittleEndianBytesToLong(data, offset, offset + arraySize) & 0xFFFFFFFFFFFFFFFEL;
                final OffsetIndexItem child = pstFile.getOffsetIndexNode(bid);
                final byte[] childData = readRaw(pstFile, child.fileOffset, child.size);
                walkXBlock(pstFile, childData, leaves);
                offset += arraySize;
            }
        } else if (data[1] == 0x01) { // XBlock: entries point to leaf data blocks
            for (int x = 0; x < numberOfEntries; x++) {
                final long bid =
                        PSTObject.convertLittleEndianBytesToLong(data, offset, offset + arraySize) & 0xFFFFFFFFFFFFFFFEL;
                final OffsetIndexItem leaf = pstFile.getOffsetIndexNode(bid);
                leaves.add(new Block(leaf.fileOffset, leaf.size));
                offset += arraySize;
            }
        }
    }

    private static byte[] readRaw(final PSTFile pstFile, final long fileOffset, final int size) throws IOException {
        return readExtent(pstFile.getFileHandle(), fileOffset, size);
    }

    // Reads exactly `size` bytes at `fileOffset`, but validates the extent against the real file
    // length first: a corrupt offset-index node can carry an absurd size, and allocating new byte[size]
    // for it would OOM and -- because OutOfMemoryError is an Error, not caught by recover()'s
    // Exception|LinkageError boundary -- abort the whole PST parse instead of skipping one attachment.
    private static byte[] readExtent(final RandomAccessFile fileHandle, final long fileOffset, final int size)
            throws IOException {
        if (size < 0 || fileOffset < 0 || fileOffset + size > fileHandle.length()) {
            throw new IOException("OST block extent out of bounds: offset=" + fileOffset
                    + " size=" + size + " fileLength=" + fileHandle.length());
        }
        final byte[] buf = new byte[size];
        fileHandle.seek(fileOffset);
        fileHandle.readFully(buf);
        return buf;
    }
}
