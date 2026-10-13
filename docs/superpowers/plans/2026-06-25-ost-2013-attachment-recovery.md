# OST-2013 Attachment Recovery (Strategy C) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Recover by-value attachments that java-libpst 0.9.3 drops on OST-2013 (type-36) files by inflating their per-block zlib compression in a pure-JVM reader, so the bytes reach the index on every OS with zero native dependency.

**Architecture:** A new `com.pff.OstCompressedBlockReader` (placed in `extract-lib`'s own source tree, same package as the frozen java-libpst, for compile-time package-private access — no reflection) re-resolves an attachment's data blocks the same way java-libpst does, then reads each block raw, decrypts it if the OST uses compressible encryption, inflates it when it carries a zlib header, concatenates, and accepts the result only when its length matches the attachment's declared size. `ResilientOutlookPSTParser` calls this from its existing recover-and-emit hook (replacing the libpff/pypff subprocess path) and records accountable recovery metadata.

**Tech Stack:** Java 21, Maven, Apache Tika 3.3.0, `com.pff:java-libpst:0.9.3` (unmodified, transitive via `tika-parser-microsoft-module`), JUnit 4, fest-assert, `java.util.zip.Inflater`.

## Global Constraints

- All new production code lives in `extract-lib`. java-libpst stays an unmodified dependency.
- The reader is placed in `package com.pff;` inside `extract-lib/src/main/java/com/pff/`. This is a deliberate classpath split-package (legal on the classpath / unnamed module; the java-libpst 0.9.3 jar is not sealed). It would break only under a JPMS module-path deployment, which Datashare does not use. No reflection, no `--add-opens`.
- Recovery is gated to OST-2013 files only (`PSTFile.getPSTFileType() == PSTFile.PST_TYPE_2013_UNICODE`, value 36); the existing gate in `ResilientOutlookPSTParser` already does this. Other PSTs never enter the reader path.
- Strict integrity gate: emit recovered bytes only when the reassembled length equals the declared attachment size; otherwise return nothing and count it as an unrecovered loss. Never index bytes that cannot be fully accounted for.
- Best-effort isolation: the reader's public entry point and the parser hook never throw into the folder walk — catch `Exception | LinkageError`, log, count as loss (the pattern already used throughout `ResilientOutlookPSTParser`).
- Encrypted (password-protected) attachments are out of scope as a recovery target: their bytes are recovered and emitted, but body extraction is left to fail; count them and move on.
- TDD: write the failing test first; commit after each green step.
- Build/test commands: `mvn -q -pl extract-lib test` (all), `mvn -q -pl extract-lib test -Dtest=ClassName#methodName` (single). Fat jar: `mvn -q -pl extract-cli -am package`.

---

## File Structure

- Create: `extract-lib/src/main/java/com/pff/OstCompressedBlockReader.java` — the reader: pure core (block read + decode + zlib inflate + size gate), block resolver (mirrors java-libpst's `loadFromOffsetItem`/`getBlockSkipPoints`), and the public `recover(PSTAttachment)` adapter.
- Create: `extract-lib/src/test/java/com/pff/OstCompressedBlockReaderTest.java` — CI unit tests for the pure core against synthetic temp-file block layouts.
- Create: `extract-lib/src/test/java/com/pff/OstByteIdentityValidationTest.java` — opt-in (local) byte-identity check of the reader against libpff truth on a real OST (milestone M1 gate); skipped when `-Dost.test.file` is absent.
- Modify: `extract-lib/src/main/java/org/icij/extract/parser/ResilientOutlookPSTParser.java` — rewire the recover-and-emit hook onto the reader; add `unrecovered`/`encrypted` accounting and metadata; drop the pst-path plumbing the libpff path needed.
- Modify: `extract-lib/src/test/java/org/icij/extract/parser/ResilientOutlookPSTParserTest.java` — assert the new metadata fields are absent on the non-OST `testPST.pst`; add the opt-in real-OST recovery integration test.
- Delete: `extract-lib/src/main/java/org/icij/extract/parser/LibpffAttachmentRecovery.java`.
- Delete: `extract-lib/src/main/resources/org/icij/extract/parser/pff_read_attachment.py` (and the now-empty `resources/org/...` tree).
- Delete: `extract-lib/src/main/resources/META-INF/services/org.apache.tika.parser.Parser` — the bogus one-line SPI file that clobbers Tika's in the fat jar.
- Modify: `extract-cli/pom.xml` — the commons-io pin (already staged in the working tree); verified by this plan.

---

## Task 1: Pure core — block read, decode, zlib inflate, size gate

**Files:**
- Create: `extract-lib/src/main/java/com/pff/OstCompressedBlockReader.java`
- Test: `extract-lib/src/test/java/com/pff/OstCompressedBlockReaderTest.java`

**Interfaces:**
- Produces:
  - `static final class OstCompressedBlockReader.Block` with `Block(long fileOffset, int size)` and package-visible final fields `long fileOffset`, `int size`.
  - `static Optional<byte[]> recoverFromBlocks(RandomAccessFile fileHandle, List<Block> blocks, boolean encrypted, long declaredSize) throws IOException` — reads each block, decrypts (compressible-encryption permutation) when `encrypted`, inflates blocks carrying a valid zlib header (raw fallback when inflation fails), and returns the bytes only when their length equals `declaredSize` (else `Optional.empty()`).

- [ ] **Step 1: Write the failing tests**

Create `extract-lib/src/test/java/com/pff/OstCompressedBlockReaderTest.java`:

```java
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
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `mvn -q -pl extract-lib test -Dtest=OstCompressedBlockReaderTest`
Expected: FAIL — compilation error, `OstCompressedBlockReader` does not exist.

- [ ] **Step 3: Write the minimal implementation**

Create `extract-lib/src/main/java/com/pff/OstCompressedBlockReader.java`:

```java
package com.pff;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
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
 * and the frozen library is consumed from the classpath, so a split package is safe here.
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
            final byte[] raw = new byte[block.size];
            fileHandle.seek(block.fileOffset);
            fileHandle.readFully(raw);
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
            while (!inflater.finished()) {
                final int n = inflater.inflate(buf);
                if (n == 0) {
                    if (inflater.finished() || inflater.needsDictionary() || inflater.needsInput()) {
                        break;
                    }
                }
                out.write(buf, 0, n);
            }
            return inflater.finished() ? out.toByteArray() : null;
        } catch (final DataFormatException e) {
            return null;
        } finally {
            inflater.end();
        }
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `mvn -q -pl extract-lib test -Dtest=OstCompressedBlockReaderTest`
Expected: PASS — all six tests green.

- [ ] **Step 5: Commit**

```bash
git add extract-lib/src/main/java/com/pff/OstCompressedBlockReader.java extract-lib/src/test/java/com/pff/OstCompressedBlockReaderTest.java
git commit -m "feat(pst): pure-JVM OST block reader core (decode + per-block zlib inflate + size gate)"
```

---

## Task 2: Block resolver — mirror java-libpst's leaf-block resolution

**Files:**
- Modify: `extract-lib/src/main/java/com/pff/OstCompressedBlockReader.java`

**Interfaces:**
- Consumes: `OstCompressedBlockReader.Block`, `recoverFromBlocks(...)` (Task 1).
- Produces: `static List<Block> resolveLeafBlocks(PSTFile pstFile, OffsetIndexItem root) throws IOException, PSTException` — returns the ordered leaf data blocks for a data node, walking XBlock/XXBlock trees exactly as java-libpst does, but without its broken decompression.

**Background (from the java-libpst 0.9.3 source):** A data node is either a single leaf block, or an "internal" block (`indexIdentifier & 2 != 0`) whose first byte is `0x01`. Internal blocks are XBlocks (`data[1] == 1`, entries point to leaf blocks) or XXBlocks (`data[1] == 2`, entries point to further XBlocks). Entry count is the little-endian 2 bytes at offset 2; each entry is an 8-byte block id (4 bytes for ANSI PSTs) at offset 8, with bit 0 cleared before lookup. This mirrors `loadFromOffsetItem` and `getBlockSkipPoints`. Internal/structural blocks are read raw (never decrypted); only leaf data blocks are decrypted, which Task 1 already handles.

- [ ] **Step 1: Write the failing test (resolver wiring smoke test)**

A faithful XBlock-tree test needs a real OST offset b-tree (resolved via `PSTFile.getOffsetIndexNode`), so the resolver's correctness is proven empirically in Task 4 (byte-identity) and Task 7 (integration). Here, add only a guard test that the method exists and treats a single non-internal leaf as one block. Append to `OstCompressedBlockReaderTest.java`:

```java
    @Test
    public void blockValueTypeHoldsOffsetAndSize() {
        final OstCompressedBlockReader.Block block = new OstCompressedBlockReader.Block(4096, 8176);
        assertThat(block.fileOffset).isEqualTo(4096L);
        assertThat(block.size).isEqualTo(8176);
    }
```

- [ ] **Step 2: Run to verify it fails**

Run: `mvn -q -pl extract-lib test -Dtest=OstCompressedBlockReaderTest#blockValueTypeHoldsOffsetAndSize`
Expected: PASS already if `Block` fields are reachable (they are, same package) — this step confirms the test compiles against the current class. If it does not compile, fix field visibility before proceeding. (The resolver itself is exercised in Tasks 4 and 7.)

- [ ] **Step 3: Add the resolver methods**

Add these imports to the top of `OstCompressedBlockReader.java`:

```java
import java.util.ArrayList;
```

Add these methods inside the class:

```java
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
        final int numberOfEntries = (int) PSTObject.convertLittleEndianBytesToLong(data, 2, 4);
        final int arraySize = pstFile.getPSTFileType() == PSTFile.PST_TYPE_ANSI ? 4 : 8;
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
        final byte[] buf = new byte[size];
        final RandomAccessFile fileHandle = pstFile.getFileHandle();
        fileHandle.seek(fileOffset);
        fileHandle.readFully(buf);
        return buf;
    }
```

- [ ] **Step 4: Run the full test class to verify green and no regressions**

Run: `mvn -q -pl extract-lib test -Dtest=OstCompressedBlockReaderTest`
Expected: PASS — seven tests green, class compiles with the new methods.

- [ ] **Step 5: Commit**

```bash
git add extract-lib/src/main/java/com/pff/OstCompressedBlockReader.java extract-lib/src/test/java/com/pff/OstCompressedBlockReaderTest.java
git commit -m "feat(pst): resolve OST leaf data blocks (XBlock/XXBlock walk) for the block reader"
```

---

## Task 3: Public adapter — `recover(PSTAttachment)`

**Files:**
- Modify: `extract-lib/src/main/java/com/pff/OstCompressedBlockReader.java`

**Interfaces:**
- Consumes: `resolveLeafBlocks(...)`, `recoverFromBlocks(...)`.
- Produces: `public static Optional<byte[]> recover(PSTAttachment attachment)` — the parser's single entry point. Never throws; returns empty on any failure. Covers both loss classes (silent IOOBE and decompress-throw) uniformly, because it re-resolves the blocks itself rather than relying on java-libpst's stream construction (which throws for the decompress class).

**Background:** `PSTAttachment.getFileInputStream()` reads the attachment data item at property id `0x3701` (PidTagAttachDataBinary). When the data is large/multi-block it is an external value reference: the descriptor is `localDescriptorItems.get(entryValueReference)`, and the data lives at `descriptor.offsetIndexIdentifier` in the offset b-tree. Small inline attachments are not a failing case and are skipped. `PSTObject.items`, `PSTObject.localDescriptorItems`, and `PSTObject.pstFile` are protected fields reachable from this same-package class.

- [ ] **Step 1: Write the failing test**

Append to `OstCompressedBlockReaderTest.java`:

```java
    @Test
    public void recoverWithNullAttachmentReturnsEmptyAndDoesNotThrow() {
        final Optional<byte[]> result = OstCompressedBlockReader.recover(null);
        assertThat(result.isPresent()).isFalse();
    }
```

- [ ] **Step 2: Run to verify it fails**

Run: `mvn -q -pl extract-lib test -Dtest=OstCompressedBlockReaderTest#recoverWithNullAttachmentReturnsEmptyAndDoesNotThrow`
Expected: FAIL — compilation error, `recover(PSTAttachment)` does not exist.

- [ ] **Step 3: Add the adapter**

Add this constant and method to `OstCompressedBlockReader.java`:

```java
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
```

- [ ] **Step 4: Run the full test class to verify green**

Run: `mvn -q -pl extract-lib test -Dtest=OstCompressedBlockReaderTest`
Expected: PASS — eight tests green.

- [ ] **Step 5: Commit**

```bash
git add extract-lib/src/main/java/com/pff/OstCompressedBlockReader.java extract-lib/src/test/java/com/pff/OstCompressedBlockReaderTest.java
git commit -m "feat(pst): public recover(PSTAttachment) adapter covering both OST loss classes"
```

---

## Task 4: Milestone M1 — byte-identity validation against libpff on a real OST

**Files:**
- Create: `extract-lib/src/test/java/com/pff/OstByteIdentityValidationTest.java`

**Interfaces:**
- Consumes: `OstCompressedBlockReader.recover(PSTAttachment)`.

**Purpose:** Empirically confirm "C reproduces libpff's bytes" for both loss classes before any irreversible rewire. This test is opt-in and runs locally against a real OST plus libpff-extracted truth files; it self-skips in CI. If it fails, fix the algorithm in Tasks 1–3 before continuing to Task 5.

**One-time local setup (document the exact ids you used in the commit message):**

```bash
# 1. Pick the small OST and export truth bytes with libpff (already-installed pff-tools).
OST=/path/to/small.ost
pffexport -q -f all -t /tmp/ost-truth "$OST"
# 2. Find one silent-class (IOOBE) and one decompress-class attachment in /tmp/ost-truth,
#    note each message descriptor node id and attachment index, and copy the truth bytes:
#    cp "/tmp/ost-truth/.../Attachment1.jpg" /tmp/truth-silent.bin
#    cp "/tmp/ost-truth/.../Attachment1.pdf" /tmp/truth-throw.bin
# 3. Record the sha256 of each truth file for the assertions:
sha256sum /tmp/truth-silent.bin /tmp/truth-throw.bin
```

- [ ] **Step 1: Write the validation test**

Create `extract-lib/src/test/java/com/pff/OstByteIdentityValidationTest.java`:

```java
package com.pff;

import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.Optional;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assume.assumeTrue;

/**
 * Opt-in byte-identity check (milestone M1): runs only when a real OST and the matching libpff truth
 * files are supplied via system properties, otherwise self-skips so CI is unaffected.
 *
 * <pre>
 * mvn -q -pl extract-lib test -Dtest=OstByteIdentityValidationTest \
 *   -Dost.test.file=/path/to.ost \
 *   -Dost.silent.descriptorId=NNNN -Dost.silent.attIndex=0 -Dost.silent.truth=/tmp/truth-silent.bin \
 *   -Dost.throw.descriptorId=MMMM -Dost.throw.attIndex=0 -Dost.throw.truth=/tmp/truth-throw.bin
 * </pre>
 */
public class OstByteIdentityValidationTest {

    @Test
    public void silentClassAttachmentMatchesLibpffBytes() throws Exception {
        assertRecoveryMatchesTruth("ost.silent.descriptorId", "ost.silent.attIndex", "ost.silent.truth");
    }

    @Test
    public void decompressClassAttachmentMatchesLibpffBytes() throws Exception {
        assertRecoveryMatchesTruth("ost.throw.descriptorId", "ost.throw.attIndex", "ost.throw.truth");
    }

    private void assertRecoveryMatchesTruth(final String idProp, final String idxProp, final String truthProp)
            throws Exception {
        final String ostPath = System.getProperty("ost.test.file");
        final String descriptorId = System.getProperty(idProp);
        final String attIndex = System.getProperty(idxProp);
        final String truthPath = System.getProperty(truthProp);
        assumeTrue(ostPath != null && descriptorId != null && attIndex != null && truthPath != null);

        final PSTFile pstFile = new PSTFile(ostPath);
        try {
            final PSTObject object =
                    PSTObject.detectAndLoadPSTObject(pstFile, Long.parseLong(descriptorId));
            assertThat(object).isInstanceOf(PSTMessage.class);
            final PSTAttachment attachment = ((PSTMessage) object).getAttachment(Integer.parseInt(attIndex));

            final Optional<byte[]> recovered = OstCompressedBlockReader.recover(attachment);
            assertThat(recovered.isPresent()).isTrue();

            final byte[] truth = Files.readAllBytes(Paths.get(truthPath));
            assertThat(recovered.get().length).isEqualTo(truth.length);
            assertThat(sha256(recovered.get())).isEqualTo(sha256(truth));
        } finally {
            if (pstFile.getFileHandle() != null) {
                pstFile.getFileHandle().close();
            }
        }
    }

    private static String sha256(final byte[] data) throws Exception {
        final byte[] digest = MessageDigest.getInstance("SHA-256").digest(data);
        final StringBuilder sb = new StringBuilder();
        for (final byte b : digest) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
```

- [ ] **Step 2: Run with the real OST and verify it passes**

Run:
```bash
mvn -q -pl extract-lib test -Dtest=OstByteIdentityValidationTest \
  -Dost.test.file="$OST" \
  -Dost.silent.descriptorId=<id> -Dost.silent.attIndex=<idx> -Dost.silent.truth=/tmp/truth-silent.bin \
  -Dost.throw.descriptorId=<id> -Dost.throw.attIndex=<idx> -Dost.throw.truth=/tmp/truth-throw.bin
```
Expected: PASS — both classes byte-identical (equal length and sha256) to libpff truth.
If FAIL: the recovered bytes diverge from libpff — fix the resolver/inflate logic in Tasks 1–3 (do not proceed to Task 5). Re-run until both pass.

- [ ] **Step 3: Verify it self-skips without the properties (CI safety)**

Run: `mvn -q -pl extract-lib test -Dtest=OstByteIdentityValidationTest`
Expected: PASS — both tests skipped (assumption not met), no failures.

- [ ] **Step 4: Commit**

```bash
git add extract-lib/src/test/java/com/pff/OstByteIdentityValidationTest.java
git commit -m "test(pst): opt-in byte-identity validation of the OST block reader vs libpff (M1)

Validated locally: silent-class descriptor <id> att <idx> and decompress-class
descriptor <id> att <idx> recover byte-identical (sha256) to libpff truth."
```

---

## Task 5: Rewire `ResilientOutlookPSTParser` and remove the libpff path

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/extract/parser/ResilientOutlookPSTParser.java`
- Delete: `extract-lib/src/main/java/org/icij/extract/parser/LibpffAttachmentRecovery.java`
- Delete: `extract-lib/src/main/resources/org/icij/extract/parser/pff_read_attachment.py`
- Modify: `extract-lib/src/test/java/org/icij/extract/parser/ResilientOutlookPSTParserTest.java`

**Interfaces:**
- Consumes: `com.pff.OstCompressedBlockReader.recover(PSTAttachment)` (returns `Optional<byte[]>`).
- Produces: metadata keys `tika:pst_attachments_recovered` (semantics: recovered by the in-JVM zlib reader), `tika:pst_attachments_unrecovered` (new), `tika:pst_attachments_encrypted` (new), set on OST-2013 root documents only. Invariant: `unreadable == recovered + unrecovered`; `encrypted <= recovered`.

- [ ] **Step 1: Write the failing test (new metadata constants + absent on non-OST)**

Add to `ResilientOutlookPSTParserTest.java`:

```java
    @Test
    public void newAttachmentMetadataIsAbsentOnNonOstPst() throws Exception {
        final ParseResult result = parseWithCounting(testPst());
        // testPST.pst is a classic (non-type-36) PST: the OST-2013 attachment fields never apply.
        assertThat(result.metadata.get(ResilientOutlookPSTParser.PST_ATTACHMENTS_UNREADABLE)).isNull();
        assertThat(result.metadata.get(ResilientOutlookPSTParser.PST_ATTACHMENTS_RECOVERED)).isNull();
        assertThat(result.metadata.get(ResilientOutlookPSTParser.PST_ATTACHMENTS_UNRECOVERED)).isNull();
        assertThat(result.metadata.get(ResilientOutlookPSTParser.PST_ATTACHMENTS_ENCRYPTED)).isNull();
    }
```

- [ ] **Step 2: Run to verify it fails**

Run: `mvn -q -pl extract-lib test -Dtest=ResilientOutlookPSTParserTest#newAttachmentMetadataIsAbsentOnNonOstPst`
Expected: FAIL — compilation error, `PST_ATTACHMENTS_UNRECOVERED` / `PST_ATTACHMENTS_ENCRYPTED` do not exist.

- [ ] **Step 3: Add the metadata constants and update docs**

In `ResilientOutlookPSTParser.java`, replace the `PST_ATTACHMENTS_RECOVERED` constant block (lines ~62-65) with:

```java
    // Count of unreadable by-value attachments whose bytes were recovered by the in-JVM zlib block
    // reader and re-emitted to the index. Set only for OST 2013 (type-36) files, alongside
    // PST_ATTACHMENTS_UNREADABLE.
    public static final String PST_ATTACHMENTS_RECOVERED = "tika:pst_attachments_recovered";
    // Count of unreadable by-value attachments the reader could not recover (failed the size gate or
    // could not be resolved). The honest residual loss: PST_ATTACHMENTS_UNREADABLE minus recovered.
    public static final String PST_ATTACHMENTS_UNRECOVERED = "tika:pst_attachments_unrecovered";
    // Count of recovered attachments whose bytes are password-protected, so the bytes are indexed but
    // body text cannot be extracted. A subset of PST_ATTACHMENTS_RECOVERED.
    public static final String PST_ATTACHMENTS_ENCRYPTED = "tika:pst_attachments_encrypted";
```

- [ ] **Step 4: Replace the recovery hook to use the reader**

In `ResilientOutlookPSTParser.java`, replace the entire `recoverAndEmitAttachment(...)` method (lines ~289-335) with:

```java
    // Fallback: java-libpst could not read this by-value attachment, but the in-JVM zlib block reader
    // can. Recover its bytes and emit them as an embedded document so the content still reaches the
    // index. Fully isolated and best-effort: when recovery fails the attachment stays counted as an
    // unrecovered loss. Gated by the same OST-2013 check as the detection above.
    private void recoverAndEmitAttachment(final PSTMessage message, final int index, final String folderPath,
                                          final EmissionContext emission) {
        final PSTAttachment attachment;
        try {
            attachment = message.getAttachment(index);
        } catch (final Exception | LinkageError e) {
            emission.incrementUnrecoveredAttachments();
            return;
        }
        final String name = safeAttachmentName(attachment);
        final Optional<byte[]> recovered = OstCompressedBlockReader.recover(attachment);
        if (recovered.isEmpty()) {
            emission.incrementUnrecoveredAttachments();
            logger.debug("OST attachment \"{}\" in folder \"{}\" could not be recovered by the zlib block reader.",
                    name, folderPath);
            return;
        }
        final byte[] bytes = recovered.get();
        final Metadata attachmentMetadata = new Metadata();
        attachmentMetadata.set(TikaCoreProperties.RESOURCE_NAME_KEY, name);
        attachmentMetadata.set(PST.PST_FOLDER_PATH, folderPath);
        try (final TikaInputStream stream = TikaInputStream.get(bytes, attachmentMetadata)) {
            emission.parseEmbedded(stream, attachmentMetadata);
            emission.incrementRecoveredAttachments();
            logger.info("Recovered attachment \"{}\" ({} bytes) in folder \"{}\" via the in-JVM zlib block reader.",
                    name, bytes.length, folderPath);
        } catch (final EncryptedDocumentException e) {
            // Bytes are byte-perfect but password-protected: indexed as an attachment, body not
            // extractable. Count as recovered and flag the encrypted residual.
            emission.incrementRecoveredAttachments();
            emission.incrementEncryptedAttachments();
            logger.info("Recovered attachment \"{}\" in folder \"{}\" is encrypted; bytes indexed, body not extractable.",
                    name, folderPath);
        } catch (final Exception | LinkageError e) {
            emission.incrementUnrecoveredAttachments();
            logger.warn("Recovered attachment \"{}\" in folder \"{}\" could not be emitted.", name, folderPath, e);
        }
    }
```

- [ ] **Step 5: Update `recordAttachmentIntegrity` to record the new fields**

Replace the `recordAttachmentIntegrity(...)` method body (lines ~445-460) with:

```java
    private void recordAttachmentIntegrity(final Metadata metadata, final String pstPath,
                                           final EmissionContext emission) {
        if (!emission.shouldCheckAttachmentIntegrity()) {
            return;
        }
        final int unreadable = emission.unreadableAttachments();
        final int recovered = emission.recoveredAttachments();
        final int unrecovered = emission.unrecoveredAttachments();
        final int encrypted = emission.encryptedAttachments();
        metadata.set(PST_ATTACHMENTS_UNREADABLE, Integer.toString(unreadable));
        metadata.set(PST_ATTACHMENTS_RECOVERED, Integer.toString(recovered));
        metadata.set(PST_ATTACHMENTS_UNRECOVERED, Integer.toString(unrecovered));
        metadata.set(PST_ATTACHMENTS_ENCRYPTED, Integer.toString(encrypted));
        if (unreadable > 0) {
            logger.warn("PST \"{}\": {} by-value attachment(s) unreadable by java-libpst (OST 2013 multi-block "
                    + "limitation); {} recovered via the in-JVM zlib block reader, {} unrecovered, {} encrypted.",
                    pstPath, unreadable, recovered, unrecovered, encrypted);
        }
    }
```

- [ ] **Step 6: Update `EmissionContext` counters and drop the pst-path plumbing**

In the `EmissionContext` class, add the two counters next to the existing `recoveredAttachments`:

```java
        private int unrecoveredAttachments = 0;
        private int encryptedAttachments = 0;
```

Add their accessors next to `incrementRecoveredAttachments()`/`recoveredAttachments()`:

```java
        private void incrementUnrecoveredAttachments() {
            unrecoveredAttachments++;
        }

        private int unrecoveredAttachments() {
            return unrecoveredAttachments;
        }

        private void incrementEncryptedAttachments() {
            encryptedAttachments++;
        }

        private int encryptedAttachments() {
            return encryptedAttachments;
        }
```

Remove the now-unused pst-path plumbing that only the libpff path needed: delete the `private String pstPath;` field, the `setPstPath(...)` method, and the `pstPath()` method from `EmissionContext`; and delete the `emission.setPstPath(pstPath);` call in `parse(...)` (the `pstPath` local there is still used for `new PSTFile(pstPath)`, so keep the local).

- [ ] **Step 7: Fix imports**

In `ResilientOutlookPSTParser.java`: add

```java
import com.pff.OstCompressedBlockReader;
import org.apache.tika.exception.EncryptedDocumentException;
```

Remove imports no longer used after the rewire: `import java.nio.file.Files;` and `import java.nio.file.Path;` (the temp-file flow is gone). Keep `import java.util.Optional;` (now `Optional<byte[]>`), `import com.pff.PSTAttachment;`, and `import com.pff.PSTNodeInputStream;` (still used by `isUnreadableByValueAttachment`).

- [ ] **Step 8: Delete the libpff path**

```bash
git rm extract-lib/src/main/java/org/icij/extract/parser/LibpffAttachmentRecovery.java
git rm extract-lib/src/main/resources/org/icij/extract/parser/pff_read_attachment.py
```

- [ ] **Step 9: Run the parser tests and the full module build**

Run: `mvn -q -pl extract-lib test -Dtest=ResilientOutlookPSTParserTest`
Expected: PASS — including `newAttachmentMetadataIsAbsentOnNonOstPst` and all pre-existing tests.

Run: `mvn -q -pl extract-lib test`
Expected: PASS — whole module green; no references to `LibpffAttachmentRecovery` remain.

- [ ] **Step 10: Commit**

```bash
git add extract-lib/src/main/java/org/icij/extract/parser/ResilientOutlookPSTParser.java extract-lib/src/test/java/org/icij/extract/parser/ResilientOutlookPSTParserTest.java
git commit -m "feat(pst): recover OST-2013 attachments via in-JVM zlib reader; drop libpff path

Rewire ResilientOutlookPSTParser's recover-and-emit hook onto OstCompressedBlockReader,
emit recovered bytes from memory, and record recovered/unrecovered/encrypted accounting.
Remove LibpffAttachmentRecovery and the pypff bridge script."
```

---

## Task 6: Make the `extract-cli` fat jar runnable for OST validation

**Files:**
- Delete: `extract-lib/src/main/resources/META-INF/services/org.apache.tika.parser.Parser`
- Modify: `extract-cli/pom.xml` (verify the commons-io pin)

**Background:** The fat jar is unrunnable because (1) `solr-solrj` pulls `commons-io` 2.8.0 which shadows Tika 3.3.0's `UnsynchronizedByteArrayInputStream.builder()`, and (2) `extract-lib`'s `META-INF/services/org.apache.tika.parser.Parser` contains a single line — `org.icij.extract.ocr.Tess4JOCRParserTest`, a non-existent (test) class — and `maven-assembly`'s `jar-with-dependencies` does not merge service files, so this clobbers Tika's, leaving `DefaultParser` with zero parsers. The OCR parser is registered programmatically in `Extractor` (`replaceParser`), so this SPI file is unnecessary; deleting it lets Tika's service file survive in the merged jar and also removes a latent `ServiceConfigurationError` in production.

- [ ] **Step 1: Verify the commons-io pin is present**

Run: `grep -n "commons-io" extract-cli/pom.xml`
Expected: shows the `commons-io` dependency pinned to `2.21.0` (already staged in the working tree). If absent, add inside `<dependencies>`:

```xml
        <!-- Tika 3.3.0 needs commons-io >= 2.12 (UnsynchronizedByteArrayInputStream.builder());
             solr-solrj pulls 2.8.0 transitively and wins mediation in the fat jar. Pin to match extract-lib. -->
        <dependency>
            <groupId>commons-io</groupId>
            <artifactId>commons-io</artifactId>
            <version>2.21.0</version>
        </dependency>
```

- [ ] **Step 2: Delete the bogus SPI service file**

```bash
git rm extract-lib/src/main/resources/META-INF/services/org.apache.tika.parser.Parser
```

- [ ] **Step 3: Build the fat jar**

Run: `mvn -q -pl extract-cli -am package -DskipTests`
Expected: BUILD SUCCESS; produces `extract-cli/target/extract-cli-*-jar-with-dependencies.jar`.

- [ ] **Step 4: Verify the fat jar loads real parsers (not zero)**

Run:
```bash
JAR=$(ls extract-cli/target/extract-cli-*-jar-with-dependencies.jar | head -1)
unzip -p "$JAR" META-INF/services/org.apache.tika.parser.Parser
```
Expected: the merged service file contains Tika's real parsers (e.g. `org.apache.tika.parser.microsoft.*`, `org.apache.tika.parser.pdf.PDFParser`), not the single bogus `Tess4JOCRParserTest` line. Many parser class names confirm the clobber is fixed. (Optional deeper check: `java -cp "$JAR" org.apache.tika.cli.TikaCLI --list-parsers | head -20`.)

- [ ] **Step 5: Commit**

```bash
git add extract-cli/pom.xml
git commit -m "build(cli): make fat jar runnable — pin commons-io 2.21.0, drop bogus Parser SPI file

The maven-assembly fat jar can't merge service files, so extract-lib's one-line
META-INF/services Parser file (pointing at a non-existent test class) clobbered
Tika's and left DefaultParser empty. The OCR parser is wired programmatically in
Extractor, so the SPI file is unnecessary; removing it lets Tika's survive."
```

---

## Task 7: OST integration test, fixture, and before/after validation

**Files:**
- Modify: `extract-lib/src/test/java/org/icij/extract/parser/ResilientOutlookPSTParserTest.java`
- (Best-effort) Create: `extract-lib/src/test/resources/documents/pst/ost2013-attachment.ost`

**Interfaces:**
- Consumes: `ResilientOutlookPSTParser`, the `CountingExtractor` already in the test, the metadata constants from Task 5.

- [ ] **Step 1: Add the opt-in real-OST recovery integration test**

Append to `ResilientOutlookPSTParserTest.java` (add imports `org.junit.Test` is present; add `import static org.junit.Assume.assumeTrue;` and `java.nio.file.Paths` is present):

```java
    @Test
    public void recoversUnreadableAttachmentsOnARealOst() throws Exception {
        // Opt-in: runs only against a real OST-2013 supplied via -Dost.test.file=<path>, else skips.
        final String ostPath = System.getProperty("ost.test.file");
        assumeTrue(ostPath != null);

        final ParseResult result = parseWithCounting(Paths.get(ostPath));

        final String unreadable = result.metadata.get(ResilientOutlookPSTParser.PST_ATTACHMENTS_UNREADABLE);
        final String recovered = result.metadata.get(ResilientOutlookPSTParser.PST_ATTACHMENTS_RECOVERED);
        final String unrecovered = result.metadata.get(ResilientOutlookPSTParser.PST_ATTACHMENTS_UNRECOVERED);
        // type-36 OST: the integrity fields must be present.
        assertThat(unreadable).isNotNull();
        assertThat(recovered).isNotNull();
        assertThat(unrecovered).isNotNull();
        // Invariant: every unreadable attachment is either recovered or counted unrecovered.
        assertThat(Integer.parseInt(unreadable))
                .isEqualTo(Integer.parseInt(recovered) + Integer.parseInt(unrecovered));
        // The corpus has known recoverable attachments, so the reader must recover at least one.
        assertThat(Integer.parseInt(recovered)).isGreaterThan(0);
    }
```

- [ ] **Step 2: Run it against the small OST and verify recovery**

Run: `mvn -q -pl extract-lib test -Dtest=ResilientOutlookPSTParserTest#recoversUnreadableAttachmentsOnARealOst -Dost.test.file=/path/to/small.ost`
Expected: PASS — `recovered > 0` and `unreadable == recovered + unrecovered`.

- [ ] **Step 3: Verify it self-skips in CI**

Run: `mvn -q -pl extract-lib test -Dtest=ResilientOutlookPSTParserTest#recoversUnreadableAttachmentsOnARealOst`
Expected: PASS — test skipped (no `ost.test.file`).

- [ ] **Step 4: Best-effort committable fixture**

Attempt to produce a small, non-sensitive OST-2013 file that reproduces the multi-block zlib defect, for an always-on CI integration test. Approaches, in order of preference:
  1. Build a minimal OST-2013 with Outlook/Exchange (or a generator) containing one message with one multi-block (>~8 KB) attachment of compressible content (so OST internal compression engages), then confirm java-libpst drops it and the reader recovers it.
  2. If no clean generator is available, minimize a real OST: delete all but one folder/message, scrub recipient/subject/body to non-sensitive placeholders, keep the failing attachment.

If a fixture under a few hundred KB with no sensitive content is achievable, commit it to `extract-lib/src/test/resources/documents/pst/ost2013-attachment.ost` and add a non-opt-in test mirroring Step 1 but loading `getClass().getResource("/documents/pst/ost2013-attachment.ost")`. If neither approach yields a safe, small fixture, STOP here and rely on the opt-in test from Step 1 — do not commit a large or sensitive OST. Record the outcome in the commit message.

- [ ] **Step 5: Before/after validation on the real OSTs (manual, local)**

Using the runnable fat jar from Task 6, run the single-file path over the local OSTs with OCR and a 6 GB heap and capture the recovery metadata:

```bash
JAR=$(ls extract-cli/target/extract-cli-*-jar-with-dependencies.jar | head -1)
# Small OST:
java -Xmx6g -cp "$JAR" org.icij.extract.cli.Main <extract-args-for-single-file> \
  /path/to/small.ost
# Big OST (insight only; output discarded):
java -Xmx6g -cp "$JAR" org.icij.extract.cli.Main <extract-args-for-single-file> \
  /path/to/big.ost
```
Expected: `tika:pst_attachments_recovered` accounts for essentially all of `tika:pst_attachments_unreadable` (residual = encrypted + any unrecovered), matching the KB baseline (~271 IOOBE + 7 decompress + 16 truncated on the big OST). Confirm a recovered JPEG decodes and a recovered PDF parses. Goal is recovery counts, not collecting files.

- [ ] **Step 6: Commit**

```bash
git add extract-lib/src/test/java/org/icij/extract/parser/ResilientOutlookPSTParserTest.java
# include the fixture only if Step 4 produced a safe, small one:
# git add extract-lib/src/test/resources/documents/pst/ost2013-attachment.ost
git commit -m "test(pst): OST-2013 attachment recovery integration test + before/after validation

Opt-in real-OST test asserts unreadable == recovered + unrecovered and recovered > 0.
Validated on a small and a big OST: <fill in recovered/unreadable counts>."
```

---

## Self-Review

**1. Spec coverage**

- Strategy C pure-JVM reader → Tasks 1–3.
- Same-package `com.pff` seam, no reflection → Tasks 1–3 (class location), Global Constraints.
- Both loss classes (silent IOOBE + decompress-throw) via uniform re-resolution → Task 3 adapter (re-resolves blocks itself; throw class never needs java-libpst's stream construction).
- OST block decode before zlib sniff (compressible encryption) → Task 1 (`PSTObject.decode`), tested in Task 1 step 1.
- Strict size-match integrity gate → Task 1 `recoverFromBlocks`, tested (`rejectsWhenReassembledSizeDoesNotMatchDeclared`).
- Per-block zlib header sniff + trial-inflate + raw fallback → Task 1 (`looksLikeZlib`, `tryInflate`), tested (`fallsBackToRawWhenZlibHeaderDoesNotInflate`).
- Rewire parser onto reader; emit from memory (TikaInputStream, no temp file) → Task 5.
- Remove `LibpffAttachmentRecovery` + bridge script → Task 5 steps 8.
- Drop libpff hedge → not built anywhere (spec §2/§12); residual made accountable via metadata → Task 5.
- Metadata: keep unreadable+recovered (semantics updated), add unrecovered+encrypted, keep reconciliation → Task 5.
- Encrypted attachments deferred (recover bytes, count, document) → Task 5 (`EncryptedDocumentException` branch, `PST_ATTACHMENTS_ENCRYPTED`).
- M1 byte-identity proof before rewire → Task 4 (gates Task 5).
- Pure-logic unit tests in CI → Task 1.
- Real/crafted OST fixture + opt-in fallback → Task 7.
- CLI fat-jar runnability (commons-io pin + service-file clobber) → Task 6.

No uncovered spec requirements.

**2. Placeholder scan**

The only intentionally-deferred items are the local validation specifics in Tasks 4 and 7 (`<id>`, `<path>`, recovered/unreadable counts to fill into commit messages) — these are real-data values that can only come from running against the user's local OSTs, not code placeholders. All code steps contain complete code.

**3. Type consistency**

- `OstCompressedBlockReader.Block(long, int)` with fields `fileOffset`/`size` — consistent across Tasks 1, 2, 3, and the unit test.
- `recoverFromBlocks(RandomAccessFile, List<Block>, boolean, long) -> Optional<byte[]>` — consistent (Tasks 1, 3).
- `resolveLeafBlocks(PSTFile, OffsetIndexItem) -> List<Block>` — consistent (Tasks 2, 3).
- `recover(PSTAttachment) -> Optional<byte[]>` — public; consumed by Task 5 via `Optional<byte[]>` (matches; the old libpff API returned `Optional<Path>`, fully replaced).
- EmissionContext methods `incrementUnrecoveredAttachments`/`unrecoveredAttachments`/`incrementEncryptedAttachments`/`encryptedAttachments` — defined in Task 5 step 6, used in steps 4 and 5.
- Metadata constants `PST_ATTACHMENTS_RECOVERED`/`_UNRECOVERED`/`_ENCRYPTED` — defined Task 5 step 3, used in steps 5 and Tasks 5/7 tests.

No inconsistencies found.
