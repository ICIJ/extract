# Extraction Progress "What's Left" Estimate Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an estimated completion signal (`233140/~310000 embeds (~75%)`) to the per-file progress heartbeat for PST/OST and ZIP/7z containers, falling back to today's count-only line everywhere else.

**Architecture:** A generic `units` pair on `ExtractionProgress` (a cheaply-countable top-level element: a PST message or an archive entry) drives a percentage. The displayed embed total is projected as `embedsParsed × expectedUnits / unitsParsed`, so the percentage reduces to `unitsParsed / expectedUnits` (real ground truth, never overshoots). The PST parser owns both ends of its count via the Tika `ParseContext`; archives get their total from a cheap pre-count in `Extractor` and their numerator from `EmbedSpawner` depth-1 embeds. A `parserTracksUnits` flag disambiguates the numerator source so the two paths never double-count.

**Tech Stack:** Java 11, Tika 2.x parsers, Apache commons-compress (ZIP/7z catalogs), JUnit 4 + FEST assertions, logback (ListAppender) for log assertions.

## Global Constraints

- Scope is PST/OST + ZIP/7z only. Every other container keeps the exact current count-only line. Verbatim fallback segment: `String.format("%d embeds", embedsParsed)`.
- No change to indexing behavior, root write order, streaming, or abort logic. This feature only reads counters and formats a log line.
- `expectedUnits` sentinel for "unknown" is `-1L`. `setExpectedUnits` is set-once (CAS from `-1`).
- All new counters are `AtomicLong`; the "owns units" flag is a `volatile boolean`. Parallel PST folder fan-out increments them concurrently.
- Archive pre-count failures (corrupt, encrypted, unknown type, no backing file) must be swallowed and leave `expectedUnits == -1`. A pre-count must never break or slow normal extraction beyond a small magic-byte read.
- `~` prefixes the projected embed total in the rendered line; the percentage is also prefixed `~`.
- All changes land in `extract-lib`; the only `datashare` change is an `extract.version` bump after release (Task 7), gated on the release existing.

---

### Task 1: `ExtractionProgress` units data model

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/extract/extractor/ExtractionProgress.java`
- Test: `extract-lib/src/test/java/org/icij/extract/extractor/ExtractionProgressTest.java` (exists; add cases)

**Interfaces:**
- Produces:
  - `boolean setExpectedUnits(long total)` — CAS from `-1`; returns `true` iff this call set it.
  - `long expectedUnits()` — current total, or `-1` if unset.
  - `void incrementUnits()`
  - `long unitsParsed()`
  - `void markParserTracksUnits()`
  - `boolean parserTracksUnits()`

- [ ] **Step 1: Write the failing test**

Add to `ExtractionProgressTest.java`:

```java
    @Test public void testExpectedUnitsDefaultsToUnknown() {
        ExtractionProgress p = new ExtractionProgress(java.nio.file.Paths.get("/x.ost"), 0L);
        assertThat(p.expectedUnits()).isEqualTo(-1L);
        assertThat(p.unitsParsed()).isEqualTo(0L);
        assertThat(p.parserTracksUnits()).isFalse();
    }

    @Test public void testSetExpectedUnitsIsSetOnce() {
        ExtractionProgress p = new ExtractionProgress(java.nio.file.Paths.get("/x.ost"), 0L);
        assertThat(p.setExpectedUnits(250_000L)).isTrue();
        assertThat(p.expectedUnits()).isEqualTo(250_000L);
        // A second call (e.g. a nested container) must not clobber the root total.
        assertThat(p.setExpectedUnits(7L)).isFalse();
        assertThat(p.expectedUnits()).isEqualTo(250_000L);
    }

    @Test public void testIncrementUnitsAndFlag() {
        ExtractionProgress p = new ExtractionProgress(java.nio.file.Paths.get("/x.ost"), 0L);
        p.incrementUnits(); p.incrementUnits();
        assertThat(p.unitsParsed()).isEqualTo(2L);
        p.markParserTracksUnits();
        assertThat(p.parserTracksUnits()).isTrue();
    }
```

(If `ExtractionProgressTest` lacks the FEST import, add `import static org.fest.assertions.Assertions.assertThat;`.)

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -pl extract-lib test -Dtest=ExtractionProgressTest`
Expected: FAIL, compilation error `cannot find symbol: method setExpectedUnits`.

- [ ] **Step 3: Write minimal implementation**

In `ExtractionProgress.java`, add fields beside the existing `AtomicLong`s:

```java
    private final AtomicLong expectedUnits = new AtomicLong(-1L);
    private final AtomicLong unitsParsed = new AtomicLong();
    private volatile boolean parserTracksUnits = false;
```

Add accessors beside the existing ones:

```java
    /** A "unit" is the container's cheaply-countable top-level element (a PST message or an
     * archive entry). Used only to project a completion percentage; -1 means unknown. */
    public boolean setExpectedUnits(final long total) { return expectedUnits.compareAndSet(-1L, total); }
    public long expectedUnits() { return expectedUnits.get(); }
    public void incrementUnits() { unitsParsed.incrementAndGet(); }
    public long unitsParsed() { return unitsParsed.get(); }
    /** Marked by a parser that supplies its own unit numerator (PST), so EmbedSpawner does not also count. */
    public void markParserTracksUnits() { parserTracksUnits = true; }
    public boolean parserTracksUnits() { return parserTracksUnits; }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q -pl extract-lib test -Dtest=ExtractionProgressTest`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add extract-lib/src/main/java/org/icij/extract/extractor/ExtractionProgress.java \
        extract-lib/src/test/java/org/icij/extract/extractor/ExtractionProgressTest.java
git commit -m "feat(progress): add units counters for completion estimate"
```

---

### Task 2: `LoggingProgressListener` estimate formatting

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/extract/extractor/LoggingProgressListener.java:17-22`
- Test: `extract-lib/src/test/java/org/icij/extract/extractor/LoggingProgressListenerTest.java` (exists; add cases)

**Interfaces:**
- Consumes (Task 1): `p.expectedUnits()`, `p.unitsParsed()`, plus existing `p.embedsParsed()`.
- Produces: unchanged `static String formatLine(ExtractionProgress p, long now)` signature.

- [ ] **Step 1: Write the failing test**

Add to `LoggingProgressListenerTest.java`:

```java
    @Test public void testFormatLineShowsEstimateWhenUnitsKnown() {
        ExtractionProgress p = new ExtractionProgress(Paths.get("/foo.ost"), 0L);
        for (int i = 0; i < 300; i++) p.incrementEmbeds();   // 300 embeds so far
        for (int i = 0; i < 3; i++) p.incrementUnits();      // 3 of ...
        p.setExpectedUnits(4L);                              // ... 4 units => 75%
        // estTotal = round(300 * 4 / 3) = 400
        assertThat(LoggingProgressListener.formatLine(p, 1_000L))
            .isEqualTo("/foo.ost: 1s, 300/~400 embeds (~75%), 0/0 OCR done, 0 skipped(maxDepth), 0 skipped(maxSize)");
    }

    @Test public void testFormatLineFallsBackWhenUnitsUnknown() {
        ExtractionProgress p = new ExtractionProgress(Paths.get("/logs.tar.gz"), 0L);
        p.incrementEmbeds();
        assertThat(LoggingProgressListener.formatLine(p, 1_000L))
            .isEqualTo("/logs.tar.gz: 1s, 1 embeds, 0/0 OCR done, 0 skipped(maxDepth), 0 skipped(maxSize)");
    }

    @Test public void testFormatLineFallsBackBeforeFirstUnit() {
        ExtractionProgress p = new ExtractionProgress(Paths.get("/foo.ost"), 0L);
        p.incrementEmbeds();
        p.setExpectedUnits(10L);   // total known, but unitsParsed == 0 -> no divide, fall back
        assertThat(LoggingProgressListener.formatLine(p, 1_000L)).contains("1 embeds,");
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -pl extract-lib test -Dtest=LoggingProgressListenerTest`
Expected: FAIL on `testFormatLineShowsEstimateWhenUnitsKnown` (still prints `300 embeds`).

- [ ] **Step 3: Write minimal implementation**

Replace the body of `formatLine` (lines 17-22) with:

```java
    static String formatLine(final ExtractionProgress p, final long now) {
        final long seconds = p.elapsedMillis(now) / 1000L;
        return String.format("%s: %ds, %s, %d/%d OCR done, %d skipped(maxDepth), %d skipped(maxSize)",
                p.path(), seconds, embedSegment(p), p.ocrCompleted(), p.ocrSubmitted(),
                p.embedsSkippedMaxDepth(), p.embedsSkippedMaxSize());
    }

    // Embed count with a projected total + percentage when the container exposes a cheap unit total
    // (PST messages, archive entries); otherwise the plain running count. The total is a projection
    // (embeds scale by the observed embeds-per-unit ratio), hence the '~'; the percentage is the real
    // unitsParsed/expectedUnits ground truth and never exceeds 100%.
    private static String embedSegment(final ExtractionProgress p) {
        final long embeds = p.embedsParsed();
        final long expectedUnits = p.expectedUnits();
        final long unitsParsed = p.unitsParsed();
        if (expectedUnits <= 0 || unitsParsed <= 0) {
            return String.format("%d embeds", embeds);
        }
        final long estTotal = Math.max(embeds, Math.round((double) embeds * expectedUnits / unitsParsed));
        final long pct = Math.min(100L, unitsParsed * 100L / expectedUnits);
        return String.format("%d/~%d embeds (~%d%%)", embeds, estTotal, pct);
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q -pl extract-lib test -Dtest=LoggingProgressListenerTest`
Expected: PASS (all cases, including the pre-existing `testFormatLine` which still hits the fallback branch).

- [ ] **Step 5: Commit**

```bash
git add extract-lib/src/main/java/org/icij/extract/extractor/LoggingProgressListener.java \
        extract-lib/src/test/java/org/icij/extract/extractor/LoggingProgressListenerTest.java
git commit -m "feat(progress): render projected embed total and percentage"
```

---

### Task 3: `ArchiveEntryCounter` cheap ZIP/7z pre-count

**Files:**
- Create: `extract-lib/src/main/java/org/icij/extract/extractor/ArchiveEntryCounter.java`
- Modify: `extract-lib/pom.xml` (add explicit `commons-compress` compile dependency)
- Test: `extract-lib/src/test/java/org/icij/extract/extractor/ArchiveEntryCounterTest.java` (create)

**Interfaces:**
- Produces: `static java.util.OptionalLong countTopLevelEntries(java.nio.file.Path path)` — non-directory entry count for ZIP/7z, empty for anything else or on any error.

- [ ] **Step 1: Add the commons-compress dependency**

`commons-compress` is only on the classpath transitively today. Add it explicitly to `extract-lib/pom.xml` in the `<dependencies>` block (let Maven pick the Tika-managed version; if the build has no dependencyManagement entry, pin `1.27.1`, which is present in `~/.m2`):

```xml
        <dependency>
            <groupId>org.apache.commons</groupId>
            <artifactId>commons-compress</artifactId>
        </dependency>
```

Verify it resolves: `mvn -q -pl extract-lib dependency:get -Dartifact=org.apache.commons:commons-compress:1.27.1 || true` then `mvn -q -pl extract-lib -o compile`.

- [ ] **Step 2: Write the failing test**

Create `ArchiveEntryCounterTest.java`:

```java
package org.icij.extract.extractor;

import org.apache.commons.compress.archivers.sevenz.SevenZArchiveEntry;
import org.apache.commons.compress.archivers.sevenz.SevenZOutputFile;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.OptionalLong;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.fest.assertions.Assertions.assertThat;

public class ArchiveEntryCounterTest {
    @Rule public TemporaryFolder tmp = new TemporaryFolder();

    private Path zipWith(int files) throws Exception {
        Path zip = tmp.newFile("a.zip").toPath();
        try (ZipOutputStream zos = new ZipOutputStream(Files.newOutputStream(zip))) {
            zos.putNextEntry(new ZipEntry("dir/"));        // directory entry: must NOT be counted
            zos.closeEntry();
            for (int i = 0; i < files; i++) {
                zos.putNextEntry(new ZipEntry("dir/f" + i + ".txt"));
                zos.write(("hello" + i).getBytes(StandardCharsets.UTF_8));
                zos.closeEntry();
            }
        }
        return zip;
    }

    @Test public void testCountsZipNonDirectoryEntries() throws Exception {
        assertThat(ArchiveEntryCounter.countTopLevelEntries(zipWith(3)).getAsLong()).isEqualTo(3L);
    }

    @Test public void testCounts7zNonDirectoryEntries() throws Exception {
        Path sevenZ = tmp.newFile("a.7z").toPath();
        try (SevenZOutputFile out = new SevenZOutputFile(sevenZ.toFile())) {
            for (int i = 0; i < 2; i++) {
                Path src = tmp.newFile("s" + i + ".txt").toPath();
                Files.write(src, ("x" + i).getBytes(StandardCharsets.UTF_8));
                SevenZArchiveEntry e = out.createArchiveEntry(src.toFile(), "s" + i + ".txt");
                out.putArchiveEntry(e);
                out.write(Files.readAllBytes(src));
                out.closeArchiveEntry();
            }
        }
        assertThat(ArchiveEntryCounter.countTopLevelEntries(sevenZ).getAsLong()).isEqualTo(2L);
    }

    @Test public void testEmptyForNonArchive() throws Exception {
        Path txt = tmp.newFile("plain.txt").toPath();
        Files.write(txt, "not an archive".getBytes(StandardCharsets.UTF_8));
        assertThat(ArchiveEntryCounter.countTopLevelEntries(txt).isPresent()).isFalse();
    }

    @Test public void testEmptyForCorruptZipMagic() throws Exception {
        Path fake = tmp.newFile("fake.zip").toPath();
        Files.write(fake, new byte[] {'P','K',3,4, 0,0,0,0}); // ZIP magic but truncated/garbage
        assertThat(ArchiveEntryCounter.countTopLevelEntries(fake).isPresent()).isFalse();
    }
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `mvn -q -pl extract-lib test -Dtest=ArchiveEntryCounterTest`
Expected: FAIL, compilation error `cannot find symbol: class ArchiveEntryCounter`.

- [ ] **Step 4: Write minimal implementation**

Create `ArchiveEntryCounter.java`:

```java
package org.icij.extract.extractor;

import org.apache.commons.compress.archivers.sevenz.SevenZArchiveEntry;
import org.apache.commons.compress.archivers.sevenz.SevenZFile;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Enumeration;
import java.util.OptionalLong;

/**
 * Cheaply counts the top-level entries of an archive whose format carries a catalog (ZIP central
 * directory, 7z header), without decompressing content. Used to give the progress heartbeat a
 * denominator. Any failure (unknown/unsupported format, corrupt or encrypted archive, no readable
 * file) yields {@link OptionalLong#empty()} so extraction proceeds with the count-only heartbeat.
 * Directory entries are excluded so the count matches the embeds Tika's PackageParser emits.
 */
public final class ArchiveEntryCounter {
    private static final Logger logger = LoggerFactory.getLogger(ArchiveEntryCounter.class);

    private ArchiveEntryCounter() {}

    public static OptionalLong countTopLevelEntries(final Path path) {
        try {
            final byte[] magic = readMagic(path, 6);
            if (isZip(magic)) {
                return countZip(path);
            }
            if (isSevenZ(magic)) {
                return countSevenZ(path);
            }
            return OptionalLong.empty();
        } catch (final Throwable t) {
            // Never let a pre-count failure affect extraction.
            logger.debug("archive pre-count skipped for {}: {}", path, t.toString());
            return OptionalLong.empty();
        }
    }

    private static byte[] readMagic(final Path path, final int n) throws IOException {
        final byte[] buf = new byte[n];
        try (InputStream in = Files.newInputStream(path)) {
            int off = 0, r;
            while (off < n && (r = in.read(buf, off, n - off)) != -1) {
                off += r;
            }
        }
        return buf;
    }

    // Local file header "PK\x03\x04", empty archive "PK\x05\x06", spanned "PK\x07\x08".
    private static boolean isZip(final byte[] m) {
        return m.length >= 4 && m[0] == 'P' && m[1] == 'K'
                && (m[2] == 3 || m[2] == 5 || m[2] == 7)
                && (m[3] == 4 || m[3] == 6 || m[3] == 8);
    }

    // 7z signature: 37 7A BC AF 27 1C
    private static boolean isSevenZ(final byte[] m) {
        return m.length >= 6 && (m[0] & 0xFF) == 0x37 && (m[1] & 0xFF) == 0x7A
                && (m[2] & 0xFF) == 0xBC && (m[3] & 0xFF) == 0xAF
                && (m[4] & 0xFF) == 0x27 && (m[5] & 0xFF) == 0x1C;
    }

    private static OptionalLong countZip(final Path path) throws IOException {
        // ZipFile reads only the central directory; no entry data is decompressed.
        try (ZipFile zip = ZipFile.builder().setPath(path).get()) {
            long count = 0;
            final Enumeration<ZipArchiveEntry> entries = zip.getEntries();
            while (entries.hasMoreElements()) {
                if (!entries.nextElement().isDirectory()) {
                    count++;
                }
            }
            return OptionalLong.of(count);
        }
    }

    private static OptionalLong countSevenZ(final Path path) throws IOException {
        try (SevenZFile sevenZ = SevenZFile.builder().setPath(path).get()) {
            long count = 0;
            for (final SevenZArchiveEntry e : sevenZ.getEntries()) {
                if (!e.isDirectory()) {
                    count++;
                }
            }
            return OptionalLong.of(count);
        }
    }
}
```

Note: `ZipFile.builder()` / `SevenZFile.builder()` require commons-compress >= 1.26. If the resolved version is older, replace with `new ZipFile(path.toFile())` and `new SevenZFile(path.toFile())` (both work but are deprecated in newer versions).

- [ ] **Step 5: Run test to verify it passes**

Run: `mvn -q -pl extract-lib test -Dtest=ArchiveEntryCounterTest`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add extract-lib/pom.xml \
        extract-lib/src/main/java/org/icij/extract/extractor/ArchiveEntryCounter.java \
        extract-lib/src/test/java/org/icij/extract/extractor/ArchiveEntryCounterTest.java
git commit -m "feat(progress): add cheap ZIP/7z entry pre-counter"
```

---

### Task 4: `EmbedSpawner` supplies the archive numerator (depth-1 embeds)

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/extract/extractor/EmbedSpawner.java` (increment site around line 361-376)
- Test: `extract-lib/src/test/java/org/icij/extract/extractor/EmbedSpawnerUnitCountTest.java` (create)

**Interfaces:**
- Consumes (Task 1): `progress.parserTracksUnits()`, `progress.incrementUnits()`, `progress.unitsParsed()`.
- Consumes existing test seams: `EmbedSpawner.pushForTest(TikaDocument)`, `parseEmbedded(InputStream, ContentHandler, Metadata, boolean)`, the depth expression `baseDepthOffset + tikaDocumentStack.size()`.
- Produces: a package-private test constructor `EmbedSpawner(TikaDocument root, ExtractionProgress progress, int maxEmbedDepth)`.

Context: a top-level archive entry is a depth-1 embed. At the increment site the current embed's depth equals `baseDepthOffset + tikaDocumentStack.size()` (the exact expression the depth guard already uses). PST does NOT rely on this: it sets `parserTracksUnits`, which suppresses this increment, and supplies its own numerator in Task 5.

- [ ] **Step 1: Write the failing test**

Create `EmbedSpawnerUnitCountTest.java`:

```java
package org.icij.extract.extractor;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.sax.BodyContentHandler;
import org.icij.extract.document.DigestIdentifier;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.TikaDocument;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;

import static org.fest.assertions.Assertions.assertThat;

public class EmbedSpawnerUnitCountTest {

    private TikaDocument root(final String path) {
        return new DocumentFactory()
                .withIdentifier(new DigestIdentifier("SHA-256", Charset.defaultCharset()))
                .create(Paths.get(path));
    }

    private Metadata nonInline(final String name) {
        final Metadata m = new Metadata();
        m.set(TikaCoreProperties.RESOURCE_NAME_KEY, name);
        return m;
    }

    @Test public void testDepthOneEmbedIncrementsUnits() throws Exception {
        final ExtractionProgress progress = new ExtractionProgress(Paths.get("/big.zip"), 0L);
        final TikaDocument root = root("/big.zip");
        // Stack holds only the root -> the next embed is at depth 1.
        final EmbedSpawner spawner = new EmbedSpawner(root, progress, 0);

        spawner.parseEmbedded(new ByteArrayInputStream("payload".getBytes(StandardCharsets.UTF_8)),
                new BodyContentHandler(), nonInline("entry1.txt"), false);

        assertThat(progress.unitsParsed()).isEqualTo(1L);
    }

    @Test public void testParserTrackedProgressIsNotDoubleCounted() throws Exception {
        final ExtractionProgress progress = new ExtractionProgress(Paths.get("/foo.ost"), 0L);
        progress.markParserTracksUnits(); // PST owns the numerator
        final TikaDocument root = root("/foo.ost");
        final EmbedSpawner spawner = new EmbedSpawner(root, progress, 0);

        spawner.parseEmbedded(new ByteArrayInputStream("msg".getBytes(StandardCharsets.UTF_8)),
                new BodyContentHandler(), nonInline("message1.eml"), false);

        assertThat(progress.unitsParsed()).isEqualTo(0L); // EmbedSpawner must not count when parser owns
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -pl extract-lib test -Dtest=EmbedSpawnerUnitCountTest`
Expected: FAIL, compilation error (no `EmbedSpawner(TikaDocument, ExtractionProgress, int)` constructor).

- [ ] **Step 3: Write minimal implementation**

(a) Add a package-private test constructor near the other test constructors (the ones at the "Serial mode with an explicit depth limit" comment). It must build the same serial, no-spool spawner those use, but with a real `progress`. Mirror the existing serial test constructor's delegation, substituting `progress` for the `null`/no-progress argument:

```java
    // Serial spawner with an explicit depth limit AND a live progress tracker (test-friendly).
    EmbedSpawner(final TikaDocument root, final ExtractionProgress progress, final int maxEmbedDepth) {
        this(root, new ParseContext(), null, w -> new BodyContentHandler(w),
                64L * 1024 * 1024, new TemporaryResources(), () -> false, progress, maxEmbedDepth);
    }
```

If no existing 9-arg constructor matches that shape, route through whichever serial test constructor exists and set the `progress` field; the required end state is: a serial spawner whose `progress` field is the passed object and whose stack initially holds only `root` (depth-1 for the first embed). Confirm against the existing `spawnerWithDepth` helper in `EmbedSpawnerDepthGuardTest` (same construction, minus progress).

(b) At the embed increment site (the `if (progress != null) { progress.incrementEmbeds(); }` block, ~line 373-376), capture the depth once and add the guarded unit increment. Replace:

```java
			if (progress != null) {
				progress.incrementEmbeds();
			}
```

with:

```java
			if (progress != null) {
				progress.incrementEmbeds();
				// A top-level archive entry is a depth-1 embed; it is the countable "unit" for the
				// completion estimate. Skip when a parser (PST) supplies its own unit numerator.
				final int embedDepth = baseDepthOffset + tikaDocumentStack.size();
				if (embedDepth == 1 && !progress.parserTracksUnits()) {
					progress.incrementUnits();
				}
			}
```

(Reuse the existing field names `baseDepthOffset` and `tikaDocumentStack` exactly as the depth guard above this block uses them.)

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q -pl extract-lib test -Dtest=EmbedSpawnerUnitCountTest,EmbedSpawnerDepthGuardTest`
Expected: PASS (depth-guard test still green: the new lines are additive and only fire on the non-refused path).

- [ ] **Step 5: Commit**

```bash
git add extract-lib/src/main/java/org/icij/extract/extractor/EmbedSpawner.java \
        extract-lib/src/test/java/org/icij/extract/extractor/EmbedSpawnerUnitCountTest.java
git commit -m "feat(progress): count depth-1 embeds as archive units"
```

---

### Task 5: PST parser publishes its message total and numerator

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/extract/parser/ResilientOutlookPSTParser.java`
  - `parse(...)` ~line 113-142 (read progress from context, pass down)
  - `extractAllMessages(...)` ~line 147-186 (claim ownership after enumeration)
  - `Reconciliation` inner class ~line 753-776 (hold optional progress, increment on emit)
- Test: `extract-lib/src/test/java/org/icij/extract/parser/ResilientOutlookPSTParserTest.java` (exists; add a case)

**Interfaces:**
- Consumes (Task 1): `progress.setExpectedUnits(long)`, `progress.markParserTracksUnits()`, `progress.incrementUnits()`.
- Consumes existing: `ParseContext`, `enumerateMessageDescriptorIds(...)` (returns `List<Integer>` or `null`), `Reconciliation.incrementEmitted()`, `emission.emittedCount()`.
- Produces: `Reconciliation.trackUnits(ExtractionProgress)`.

Ownership rule: the top-level, measurable PST wins `setExpectedUnits` (CAS from -1); it then marks `parserTracksUnits` and points its own (per-parse) `Reconciliation` at the progress. A nested PST-in-PST has its own `Reconciliation`, and `setExpectedUnits` returns false for it, so it never tracks units. This makes correctness independent of whether the embedded ParseContext carries the progress object.

- [ ] **Step 1: Write the failing test**

Add to `ResilientOutlookPSTParserTest.java`. Extend the harness to install an `ExtractionProgress` in the context, then assert the counts. Add this helper + test:

```java
    private org.icij.extract.extractor.ExtractionProgress parseWithProgress(Path file) throws Exception {
        ResilientOutlookPSTParser parser = new ResilientOutlookPSTParser();
        CountingExtractor counting = new CountingExtractor();
        org.icij.extract.extractor.ExtractionProgress progress =
                new org.icij.extract.extractor.ExtractionProgress(file, 0L);
        ParseContext context = new ParseContext();
        context.set(Parser.class, parser);
        context.set(EmbeddedDocumentExtractor.class, counting);
        context.set(org.icij.extract.extractor.ExtractionProgress.class, progress);
        Metadata metadata = new Metadata();
        try (InputStream is = TikaInputStream.get(file, metadata)) {
            parser.parse(is, new BodyContentHandler(-1), metadata, context);
        }
        return progress;
    }

    @Test public void testPstPublishesMessageTotalAndNumerator() throws Exception {
        org.icij.extract.extractor.ExtractionProgress progress = parseWithProgress(testPst());
        assertThat(progress.parserTracksUnits()).isTrue();
        assertThat(progress.expectedUnits()).isGreaterThan(0L);
        assertThat(progress.unitsParsed()).isGreaterThan(0L);
        // The numerator equals the parser's own emitted-message count and never exceeds the total.
        assertThat(progress.unitsParsed()).isLessThanOrEqualTo(progress.expectedUnits());
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -pl extract-lib test -Dtest=ResilientOutlookPSTParserTest#testPstPublishesMessageTotalAndNumerator`
Expected: FAIL, `expected:<true> but was:<false>` on `parserTracksUnits()` (nothing publishes yet).

- [ ] **Step 3: Write minimal implementation**

(a) In `Reconciliation` (line 753 block), add the optional progress link and increment on emit. Add a field and setter, and extend `incrementEmitted()`:

```java
        private volatile org.icij.extract.extractor.ExtractionProgress unitProgress;
        void trackUnits(final org.icij.extract.extractor.ExtractionProgress p) { this.unitProgress = p; }
        void incrementEmitted() {
            emittedCount.incrementAndGet();
            final org.icij.extract.extractor.ExtractionProgress p = unitProgress;
            if (p != null) {
                p.incrementUnits();
            }
        }
```

(Delete the old one-line `void incrementEmitted() { emittedCount.incrementAndGet(); }`.)

(b) In `parse(...)`, read the progress from the context and pass it to `extractAllMessages`. After the `final PstFanoutConfig fanout = context.get(PstFanoutConfig.class);` line add:

```java
        final org.icij.extract.extractor.ExtractionProgress progress =
                context.get(org.icij.extract.extractor.ExtractionProgress.class);
```

Change the `extractAllMessages(...)` call to pass it:

```java
            extractAllMessages(pstFile, pstPath, emission, metadata, reconciliation, fanout, progress);
```

(c) Change the `extractAllMessages` signature to accept the progress and claim ownership right after enumeration. Update the signature (line 147-150) to add `final org.icij.extract.extractor.ExtractionProgress progress` as the last parameter, and immediately after `final List<Integer> messageDescriptorIds = enumerateMessageDescriptorIds(pstFile, pstPath);` (line 158) add:

```java
        // Publish the message total for the progress estimate. Set-once (CAS): only the top-level,
        // measurable PST wins ownership; a nested PST-in-PST loses and never tracks units. On win we
        // mark parserTracksUnits (so EmbedSpawner does not also count) and point THIS parse's
        // Reconciliation at the progress, so every emitted message increments the numerator.
        if (progress != null && messageDescriptorIds != null
                && progress.setExpectedUnits(messageDescriptorIds.size())) {
            progress.markParserTracksUnits();
            reconciliation.trackUnits(progress);
        }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q -pl extract-lib test -Dtest=ResilientOutlookPSTParserTest`
Expected: PASS (new case + all pre-existing PST cases still green).

- [ ] **Step 5: Commit**

```bash
git add extract-lib/src/main/java/org/icij/extract/parser/ResilientOutlookPSTParser.java \
        extract-lib/src/test/java/org/icij/extract/parser/ResilientOutlookPSTParserTest.java
git commit -m "feat(progress): publish PST message total and live numerator"
```

---

### Task 6: `Extractor` wiring (context channel + archive pre-count)

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/extract/extractor/Extractor.java` (SPAWN branch of `getTikaDocument`, ~line 962-981; add a package-private helper)
- Test: `extract-lib/src/test/java/org/icij/extract/extractor/ExtractorArchiveUnitsTest.java` (create)

**Interfaces:**
- Consumes (Task 1): `ExtractionProgress.setExpectedUnits(long)`.
- Consumes (Task 3): `ArchiveEntryCounter.countTopLevelEntries(Path)`.
- Produces: package-private `void applyArchiveUnitCount(Path path, ExtractionProgress progress)` on `Extractor`.

- [ ] **Step 1: Write the failing test**

Create `ExtractorArchiveUnitsTest.java`:

```java
package org.icij.extract.extractor;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.fest.assertions.Assertions.assertThat;

public class ExtractorArchiveUnitsTest {
    @Rule public TemporaryFolder tmp = new TemporaryFolder();

    @Test public void testApplyArchiveUnitCountSetsZipEntryTotal() throws Exception {
        Path zip = tmp.newFile("a.zip").toPath();
        try (ZipOutputStream zos = new ZipOutputStream(Files.newOutputStream(zip))) {
            for (int i = 0; i < 4; i++) {
                zos.putNextEntry(new ZipEntry("f" + i + ".txt"));
                zos.write(("v" + i).getBytes(StandardCharsets.UTF_8));
                zos.closeEntry();
            }
        }
        ExtractionProgress progress = new ExtractionProgress(zip, 0L);
        new Extractor().applyArchiveUnitCount(zip, progress);
        assertThat(progress.expectedUnits()).isEqualTo(4L);
    }

    @Test public void testApplyArchiveUnitCountLeavesNonArchiveUnknown() throws Exception {
        Path txt = tmp.newFile("plain.txt").toPath();
        Files.write(txt, "hi".getBytes(StandardCharsets.UTF_8));
        ExtractionProgress progress = new ExtractionProgress(txt, 0L);
        new Extractor().applyArchiveUnitCount(txt, progress);
        assertThat(progress.expectedUnits()).isEqualTo(-1L);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -pl extract-lib test -Dtest=ExtractorArchiveUnitsTest`
Expected: FAIL, compilation error `cannot find symbol: method applyArchiveUnitCount`.

- [ ] **Step 3: Write minimal implementation**

(a) Add the helper method to `Extractor` (near `getTikaDocument`):

```java
    // Best-effort: give the progress heartbeat a denominator for catalog archives (ZIP/7z). Any
    // failure leaves expectedUnits unknown (-1) and the heartbeat stays count-only. Package-private
    // for direct testing.
    void applyArchiveUnitCount(final Path path, final ExtractionProgress progress) {
        ArchiveEntryCounter.countTopLevelEntries(path).ifPresent(progress::setExpectedUnits);
    }
```

(b) In the `EmbedHandling.SPAWN` branch of `getTikaDocument`, right after `final ExtractionProgress currentProgress = progressTracker.get(path);` (line ~966), publish the progress into the parse context and run the archive pre-count:

```java
            if (currentProgress != null) {
                // PST parser reads this back from the context to publish its message total/numerator.
                context.set(ExtractionProgress.class, currentProgress);
                // Archives (ZIP/7z) get their total here; EmbedSpawner supplies their numerator.
                applyArchiveUnitCount(path, currentProgress);
            }
```

(`ExtractionProgress` is in the same package, so no import is needed. If `Path` is not already imported in `Extractor.java`, it is — the method signatures use it.)

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q -pl extract-lib test -Dtest=ExtractorArchiveUnitsTest`
Expected: PASS.

- [ ] **Step 5: Full-module regression**

Run: `mvn -q -pl extract-lib test`
Expected: PASS. Pay attention to `EmbedSpawner*`, `ResilientOutlookPSTParser*`, `LoggingProgressListenerTest`, `ExtractionProgress*`, and any `Extractor*` tests.

- [ ] **Step 6: Commit**

```bash
git add extract-lib/src/main/java/org/icij/extract/extractor/Extractor.java \
        extract-lib/src/test/java/org/icij/extract/extractor/ExtractorArchiveUnitsTest.java
git commit -m "feat(progress): wire progress into parse context and archive pre-count"
```

---

### Task 7: datashare `extract.version` bump (gated on an extract release)

**Files:**
- Modify: `../datashare/pom.xml:99` (`<extract.version>`)

**Precondition:** Tasks 1-6 are merged to extract `master` AND a new extract-lib version has been released to the Maven repository. Do NOT invent a version; use the actual released one. This task produces no test of its own; the datashare heartbeat only logs what extract emits.

- [ ] **Step 1: Confirm the released version**

Run: `git tag --sort=-creatordate | head -3`
Use the newest release tag that contains Tasks 1-6.

- [ ] **Step 2: Bump the property**

In `../datashare/pom.xml`, change `<extract.version>9.15.2</extract.version>` to the released version from Step 1.

- [ ] **Step 3: Build datashare to confirm resolution**

Run: `cd ../datashare && mvn -q -o -pl datashare-index -am compile` (add `-U` and drop `-o` if the artifact is only in a remote repo).
Expected: resolves and compiles.

- [ ] **Step 4: Commit (datashare repo, on a branch)**

```bash
cd ../datashare
git checkout -b chore/bump-extract-progress-estimate
git add pom.xml
git commit -m "chore(deps): bump extract-lib for progress completion estimate"
```

---

## Self-Review

**Spec coverage:**
- Estimated embed total + percentage display -> Task 2.
- Generic `units` pair + `parserTracksUnits` flag + set-once -> Task 1.
- PST via ParseContext (approach A), total + numerator, top-level/nested ownership -> Task 5 (+ context.set in Task 6).
- ZIP/7z cheap pre-count, directory-entry exclusion, swallow failures -> Task 3; wired -> Task 6.
- Archive numerator from EmbedSpawner depth-1, guarded by flag -> Task 4.
- Fallback everywhere else -> Task 2 (`embedSegment` else-branch), and Tasks 3/6 leaving `-1`.
- Early-parse (`unitsParsed==0`) divide-by-zero guard -> Task 2.
- Nested container set-once guard -> Task 1 (CAS) + Task 5 (ownership) + Task 4 fixture.
- datashare bump -> Task 7.
- Verification checkpoint (one ZIP entry == one depth-1 embed) -> Task 4's `testDepthOneEmbedIncrementsUnits` asserts the depth-1 increment; Task 3 asserts the ZIP count excludes directories. The end-to-end equality (real PackageParser emits exactly the counted entries) is exercised by the Task 6 full-module regression against any archive fixtures present; if a discrepancy appears, adjust `ArchiveEntryCounter`'s directory/entry filter to match the emitted set.

**Placeholder scan:** No TBD/TODO. The two conditionals (commons-compress builder vs deprecated constructor in Task 3; the exact serial test-constructor delegation in Task 4) are real compatibility notes with concrete both-branch instructions, not deferred work.

**Type consistency:** `setExpectedUnits`/`expectedUnits`/`incrementUnits`/`unitsParsed`/`markParserTracksUnits`/`parserTracksUnits` are used identically across Tasks 1, 2, 4, 5, 6. `ArchiveEntryCounter.countTopLevelEntries` returns `OptionalLong` in Tasks 3 and 6. `applyArchiveUnitCount(Path, ExtractionProgress)` matches between Task 6 impl and test. `Reconciliation.trackUnits` / `incrementEmitted` consistent in Task 5.
