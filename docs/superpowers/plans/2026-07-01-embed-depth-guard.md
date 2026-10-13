# Max Embed-Depth Guard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Guard against decompression-bomb archives by refusing to descend into embedded documents nested deeper than a configurable limit, recording each skip instead of losing it silently.

**Architecture:** Enforce a maximum nesting depth in `EmbedSpawner.parseEmbedded` (the Tika `EmbeddedDocumentExtractor` that already tracks depth via `tikaDocumentStack`). On the non-inline spawn branch, before any spool or recursion, refuse embeds whose depth exceeds `maxEmbedDepth`. A refused embed is not spooled, not added, and not recursed into; it leaves an aggregate marker on the parent document's metadata plus a per-file progress counter.

**Tech Stack:** Java 17, Apache Tika 3.x, Maven multi-module (`extract-lib`), JUnit 4 + FEST assertions (`org.fest.assertions.Assertions.assertThat`).

## Global Constraints

- Do NOT bump the extract version.
- Byte-identity invariant: embed ids, `X-TIKA:digest:*` values, and artifact filenames MUST be unaffected for any embed that is NOT refused. The guard must be a pure no-op for within-limit embeds.
- Commit with plain git, semantic-tag messages, NO Claude attribution (no co-author trailers, no "Generated with Claude").
- This work modifies `EmbedSpawner`, which the unmerged `feat/pst-folder-fanout` branch also changes. Sequence AFTER that branch merges, or rebase on it.
- Default depth `maxEmbedDepth = 20`, on by default, `0` disables.
- Neutral metadata namespace: `X-EXTRACT:` (no org-specific naming).

**Spec:** `docs/superpowers/specs/2026-07-01-embed-depth-guard-design.md`

## File Structure

- `extract-lib/src/main/java/org/icij/extract/extractor/EmbedSpawner.java` — add depth predicate, `maxEmbedDepth` field + constructor plumbing + fork threading, `EMBEDS_SKIPPED_MAX_DEPTH` constant, `recordSkippedAtDepth`, and the guard in `parseEmbedded`.
- `extract-lib/src/main/java/org/icij/extract/extractor/ExtractionProgress.java` — add `embedsSkippedMaxDepth` counter.
- `extract-lib/src/main/java/org/icij/extract/extractor/LoggingProgressListener.java` — surface the skipped count in the heartbeat line.
- `extract-lib/src/main/java/org/icij/extract/extractor/Extractor.java` — add `maxEmbedDepth` option, field, parse, getter, and wire into `getTikaDocument`.
- Tests (new/modified):
  - `EmbedSpawnerEligibilityTest.java` (modify) — add depth-predicate unit tests, or a new `EmbedSpawnerDepthPredicateTest.java`.
  - `EmbedSpawnerDepthGuardTest.java` (new) — refuse-path behaviour.
  - `EmbedSpawnerForkTest.java` (modify) — fork inherits depth.
  - `ExtractionProgressTest.java` (modify) — new counter.
  - `LoggingProgressListenerTest.java` (modify) — heartbeat line.
  - `ExtractorTest.java` or nearest existing Extractor option test (modify/new) — option parsing.

**Test run command (all tasks):** `mvn -pl extract-lib -am test -Dtest=<ClassName>` from the repo root. Full module: `mvn -pl extract-lib -am test`.

---

### Task 1: Depth predicate on EmbedSpawner

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/extract/extractor/EmbedSpawner.java`
- Test: `extract-lib/src/test/java/org/icij/extract/extractor/EmbedSpawnerEligibilityTest.java`

**Interfaces:**
- Produces: `static boolean EmbedSpawner.exceedsMaxEmbedDepth(int stackSize, int maxEmbedDepth)` — returns `true` when the guard should refuse. Contract: `maxEmbedDepth <= 0` disables (always `false`); otherwise `stackSize > maxEmbedDepth`.

- [ ] **Step 1: Write the failing tests**

Add to `EmbedSpawnerEligibilityTest.java`:

```java
@Test public void testDepthDisabledWhenMaxIsZero() {
    assertThat(EmbedSpawner.exceedsMaxEmbedDepth(9999, 0)).isFalse();
}

@Test public void testDepthDisabledWhenMaxIsNegative() {
    assertThat(EmbedSpawner.exceedsMaxEmbedDepth(9999, -1)).isFalse();
}

@Test public void testDepthWithinLimitIsAllowed() {
    // stack size 20 with max 20: the boundary embed is allowed.
    assertThat(EmbedSpawner.exceedsMaxEmbedDepth(20, 20)).isFalse();
}

@Test public void testDepthBeyondLimitIsRefused() {
    // stack size 21 with max 20: the child at depth 21 is refused.
    assertThat(EmbedSpawner.exceedsMaxEmbedDepth(21, 20)).isTrue();
}

@Test public void testDepthOneBelowLimitIsAllowed() {
    assertThat(EmbedSpawner.exceedsMaxEmbedDepth(1, 20)).isFalse();
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn -pl extract-lib -am test -Dtest=EmbedSpawnerEligibilityTest`
Expected: FAIL — compilation error, `exceedsMaxEmbedDepth` is undefined.

- [ ] **Step 3: Implement the predicate**

In `EmbedSpawner.java`, next to the existing `static boolean isOcrEligible(...)` method (around line 657), add:

```java
// True when an embed at the given stack depth must be refused for exceeding the nesting
// limit. maxEmbedDepth <= 0 disables the guard. An embed at nesting depth d is entered with
// tikaDocumentStack.size() == d (the stack holds the root plus its d-1 embed ancestors), so
// "stackSize > maxEmbedDepth" allows depths 1..maxEmbedDepth and refuses maxEmbedDepth+1 down.
static boolean exceedsMaxEmbedDepth(final int stackSize, final int maxEmbedDepth) {
    return maxEmbedDepth > 0 && stackSize > maxEmbedDepth;
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn -pl extract-lib -am test -Dtest=EmbedSpawnerEligibilityTest`
Expected: PASS (all tests, including the pre-existing OCR-eligibility ones).

- [ ] **Step 5: Commit**

```bash
git add extract-lib/src/main/java/org/icij/extract/extractor/EmbedSpawner.java \
        extract-lib/src/test/java/org/icij/extract/extractor/EmbedSpawnerEligibilityTest.java
git commit -m "feat(extractor): add embed nesting-depth predicate"
```

---

### Task 2: Skipped-embed counter on ExtractionProgress

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/extract/extractor/ExtractionProgress.java`
- Test: `extract-lib/src/test/java/org/icij/extract/extractor/ExtractionProgressTest.java`

**Interfaces:**
- Produces: `void ExtractionProgress.incrementEmbedsSkippedMaxDepth()` and `long ExtractionProgress.embedsSkippedMaxDepth()`.

- [ ] **Step 1: Write the failing test**

Add to `ExtractionProgressTest.java`:

```java
@Test public void testEmbedsSkippedMaxDepthStartsAtZeroAndIncrements() {
    ExtractionProgress p = new ExtractionProgress(Paths.get("/x.ost"), 1_000L);
    assertThat(p.embedsSkippedMaxDepth()).isEqualTo(0L);
    p.incrementEmbedsSkippedMaxDepth();
    p.incrementEmbedsSkippedMaxDepth();
    assertThat(p.embedsSkippedMaxDepth()).isEqualTo(2L);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl extract-lib -am test -Dtest=ExtractionProgressTest`
Expected: FAIL — compilation error, `embedsSkippedMaxDepth` / `incrementEmbedsSkippedMaxDepth` undefined.

- [ ] **Step 3: Implement the counter**

In `ExtractionProgress.java`, add the field alongside the existing `AtomicLong` fields (after line 12):

```java
    private final AtomicLong embedsSkippedMaxDepth = new AtomicLong();
```

Add the getter (after `ocrCompleted()`, line 23) and the increment (after `incrementOcrCompleted()`, line 26):

```java
    public long embedsSkippedMaxDepth() { return embedsSkippedMaxDepth.get(); }
```

```java
    public void incrementEmbedsSkippedMaxDepth() { embedsSkippedMaxDepth.incrementAndGet(); }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -pl extract-lib -am test -Dtest=ExtractionProgressTest`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add extract-lib/src/main/java/org/icij/extract/extractor/ExtractionProgress.java \
        extract-lib/src/test/java/org/icij/extract/extractor/ExtractionProgressTest.java
git commit -m "feat(extractor): count embeds skipped for max depth"
```

---

### Task 3: Wire the guard into EmbedSpawner

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/extract/extractor/EmbedSpawner.java`
- Test: `extract-lib/src/test/java/org/icij/extract/extractor/EmbedSpawnerDepthGuardTest.java` (new)
- Test: `extract-lib/src/test/java/org/icij/extract/extractor/EmbedSpawnerForkTest.java` (modify)

**Interfaces:**
- Consumes: `EmbedSpawner.exceedsMaxEmbedDepth(int, int)` (Task 1), `ExtractionProgress.incrementEmbedsSkippedMaxDepth()` (Task 2).
- Produces:
  - `static final String EmbedSpawner.EMBEDS_SKIPPED_MAX_DEPTH = "X-EXTRACT:embedsSkippedMaxDepth"`
  - `static final int EmbedSpawner.DEFAULT_MAX_EMBED_DEPTH = 20`
  - New master-constructor trailing parameter `final int maxEmbedDepth` (now the last parameter).
  - New package-private serial test constructor with an explicit `int maxEmbedDepth`.
  - Test accessor `int EmbedSpawner.maxEmbedDepthForTest()`.

- [ ] **Step 1: Write the failing tests**

Create `extract-lib/src/test/java/org/icij/extract/extractor/EmbedSpawnerDepthGuardTest.java`:

```java
package org.icij.extract.extractor;

import org.apache.tika.io.TemporaryResources;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
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

public class EmbedSpawnerDepthGuardTest {

    private TikaDocument root(final String path) {
        return new DocumentFactory()
                .withIdentifier(new DigestIdentifier("SHA-256", Charset.defaultCharset()))
                .create(Paths.get(path));
    }

    // Serial spawner with an explicit, small depth limit and no output path (no spooling).
    private EmbedSpawner spawnerWithDepth(final TikaDocument root, final int maxEmbedDepth) {
        return new EmbedSpawner(root, new ParseContext(), null,
                w -> new BodyContentHandler(w),
                64L * 1024 * 1024, new TemporaryResources(), () -> false, maxEmbedDepth);
    }

    private Metadata nonInline(final String name) {
        final Metadata m = new Metadata();
        m.set(org.apache.tika.metadata.TikaCoreProperties.RESOURCE_NAME_KEY, name);
        return m;
    }

    @Test
    public void testEmbedBeyondDepthIsRefusedAndRecorded() throws Exception {
        final TikaDocument root = root("/tmp/fake-root.zip");
        final EmbedSpawner spawner = spawnerWithDepth(root, 1);
        // Push one document so the stack size becomes 2 (> maxEmbedDepth of 1): the next embed is refused.
        final TikaDocument parent = new DocumentFactory()
                .withIdentifier(new DigestIdentifier("SHA-256", StandardCharsets.UTF_8))
                .create(Paths.get("/tmp/parent"));
        spawner.pushForTest(parent);

        spawner.parseEmbedded(new ByteArrayInputStream("payload".getBytes(StandardCharsets.UTF_8)),
                new BodyContentHandler(), nonInline("bomb.bin"), false);

        // Refused: no embed added to the parent, and the skip is recorded on the parent + progress-free path.
        assertThat(parent.getEmbeds()).isEmpty();
        assertThat(parent.getMetadata().get(EmbedSpawner.EMBEDS_SKIPPED_MAX_DEPTH)).isEqualTo("1");
    }

    @Test
    public void testSecondRefusedEmbedIncrementsParentMarker() throws Exception {
        final TikaDocument root = root("/tmp/fake-root.zip");
        final EmbedSpawner spawner = spawnerWithDepth(root, 1);
        final TikaDocument parent = new DocumentFactory()
                .withIdentifier(new DigestIdentifier("SHA-256", StandardCharsets.UTF_8))
                .create(Paths.get("/tmp/parent"));
        spawner.pushForTest(parent);

        spawner.parseEmbedded(new ByteArrayInputStream("a".getBytes(StandardCharsets.UTF_8)),
                new BodyContentHandler(), nonInline("bomb1.bin"), false);
        spawner.parseEmbedded(new ByteArrayInputStream("b".getBytes(StandardCharsets.UTF_8)),
                new BodyContentHandler(), nonInline("bomb2.bin"), false);

        assertThat(parent.getMetadata().get(EmbedSpawner.EMBEDS_SKIPPED_MAX_DEPTH)).isEqualTo("2");
        assertThat(parent.getEmbeds()).isEmpty();
    }

    @Test
    public void testGuardDisabledWhenDepthIsZero() throws Exception {
        final TikaDocument root = root("/tmp/fake-root.zip");
        final EmbedSpawner spawner = spawnerWithDepth(root, 0); // disabled
        final TikaDocument parent = new DocumentFactory()
                .withIdentifier(new DigestIdentifier("SHA-256", StandardCharsets.UTF_8))
                .create(Paths.get("/tmp/parent"));
        spawner.pushForTest(parent);

        spawner.parseEmbedded(new ByteArrayInputStream("payload".getBytes(StandardCharsets.UTF_8)),
                new BodyContentHandler(), nonInline("real.bin"), false);

        // Not refused: nothing recorded on the parent (the embed took the normal spawn path).
        assertThat(parent.getMetadata().get(EmbedSpawner.EMBEDS_SKIPPED_MAX_DEPTH)).isNull();
    }
}
```

Note: `testGuardDisabledWhenDepthIsZero` drives the real spawn path with `outputPath == null` and a plain byte stream. `spawnEmbedded` buffers text and calls `addEmbed`; with no output path it does not spool to disk. The assertion only checks that the skip marker is absent, which is robust regardless of whether the trivial payload parses.

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn -pl extract-lib -am test -Dtest=EmbedSpawnerDepthGuardTest`
Expected: FAIL — compilation error: the 8-arg serial constructor and `EMBEDS_SKIPPED_MAX_DEPTH` do not exist yet.

- [ ] **Step 3: Add the constant, field, constructors, and fork threading**

In `EmbedSpawner.java`:

a) Add constants near the top of the class body (after the logger, around line 50):

```java
    // Default maximum embed nesting depth; embeds deeper than this are refused (see exceedsMaxEmbedDepth).
    static final int DEFAULT_MAX_EMBED_DEPTH = 20;
    // Metadata key set on a parent document, counting its direct children refused for exceeding the depth limit.
    static final String EMBEDS_SKIPPED_MAX_DEPTH = "X-EXTRACT:embedsSkippedMaxDepth";
```

b) Add the field alongside the other final fields (e.g. after `legacyUntitledNaming`, around line 63):

```java
    private final int maxEmbedDepth;
```

c) Update the serial convenience constructor (currently lines 97-105) to pass the default and add an explicit-depth sibling. Replace the existing serial constructor with:

```java
	EmbedSpawner(final TikaDocument root, final ParseContext context, final Path outputPath,
				 final Function<Writer, ContentHandler> handlerFunction,
				 final long embedMemoryBudgetBytes, final TemporaryResources tmp,
				 final BooleanSupplier memoryPressureHigh) {
		this(root, context, outputPath, handlerFunction, embedMemoryBudgetBytes, tmp, memoryPressureHigh,
				DEFAULT_MAX_EMBED_DEPTH);
	}

	// Serial mode with an explicit depth limit (test-friendly). No fan-out, no OCR.
	EmbedSpawner(final TikaDocument root, final ParseContext context, final Path outputPath,
				 final Function<Writer, ContentHandler> handlerFunction,
				 final long embedMemoryBudgetBytes, final TemporaryResources tmp,
				 final BooleanSupplier memoryPressureHigh, final int maxEmbedDepth) {
		this(root, context, outputPath, handlerFunction, embedMemoryBudgetBytes, tmp, memoryPressureHigh,
				() -> null, false, null, null, false, 0L, null, null, false, maxEmbedDepth);
	}
```

d) Update the `ExecutorService` convenience constructor (currently lines 107-121) to pass `DEFAULT_MAX_EMBED_DEPTH` as the new trailing argument to the master constructor. Change its delegated `this(...)` call's final line from:

```java
				() -> ocrExecutor, ocrExecutor != null, progress, digester, ocrFanout, ocrMinImageBytes,
				ocrParserClassName, null, false);
```

to:

```java
				() -> ocrExecutor, ocrExecutor != null, progress, digester, ocrFanout, ocrMinImageBytes,
				ocrParserClassName, null, false, DEFAULT_MAX_EMBED_DEPTH);
```

e) Update the master constructor (currently lines 123-152) to accept and assign `maxEmbedDepth`. Add `final int maxEmbedDepth` as the last parameter of its signature, and add the assignment in the body next to the other assignments (e.g. after `this.legacyUntitledNaming = legacyUntitledNaming;`):

```java
		this.maxEmbedDepth = maxEmbedDepth;
```

f) Update the fork copy-constructor (currently lines 158-179) to inherit the limit. Add after `this.legacyUntitledNaming = template.legacyUntitledNaming;`:

```java
		this.maxEmbedDepth = template.maxEmbedDepth; // forks enforce the same depth on their own stack
```

g) Add the test accessor next to the other test accessors (after `stackDepth()`, around line 187):

```java
	// Test accessor (package-private): the configured maximum embed nesting depth.
	int maxEmbedDepthForTest() { return maxEmbedDepth; }
```

- [ ] **Step 4: Add the guard and the recording helper**

In `parseEmbedded`, the non-inline `else` branch currently begins (around line 239-242) with:

```java
		} else {
			if (progress != null) {
				progress.incrementEmbeds();
			}
```

Insert the guard as the very first statement inside that `else`, before `incrementEmbeds()`:

```java
		} else {
			// Decompression-bomb guard: refuse embeds nested deeper than the limit BEFORE any spool or
			// recursion. Refused embeds are not spooled, not added, and not recursed into; the skip is
			// recorded on the parent and counted, so it is visible/alertable rather than silently lost.
			if (exceedsMaxEmbedDepth(tikaDocumentStack.size(), maxEmbedDepth)) {
				recordSkippedAtDepth(metadata);
				return;
			}
			if (progress != null) {
				progress.incrementEmbeds();
			}
```

Add the helper method near the other private methods (e.g. after `nextUntitledName()`, around line 213):

```java
	// Record a refused embed on its parent: bump the parent's aggregate skip counter (indexed marker)
	// and the per-file progress counter. Runs on the walk thread for this parent's subtree, so the
	// read-increment-set is single-threaded with respect to this parent (consistent with how other
	// embed metadata is set during the walk).
	private void recordSkippedAtDepth(final Metadata skipped) {
		final Metadata parent = tikaDocumentStack.getLast().getMetadata();
		int count = 0;
		final String existing = parent.get(EMBEDS_SKIPPED_MAX_DEPTH);
		if (existing != null) {
			try {
				count = Integer.parseInt(existing);
			} catch (final NumberFormatException ignored) {
				// Treat an unparseable marker as zero and overwrite it.
			}
		}
		parent.set(EMBEDS_SKIPPED_MAX_DEPTH, Integer.toString(count + 1));
		if (progress != null) {
			progress.incrementEmbedsSkippedMaxDepth();
		}
		logger.warn("Skipping embed \"{}\" nested beyond max depth {} (in \"{}\").",
				skipped.get(TikaCoreProperties.RESOURCE_NAME_KEY), maxEmbedDepth, root);
	}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `mvn -pl extract-lib -am test -Dtest=EmbedSpawnerDepthGuardTest`
Expected: PASS.

- [ ] **Step 6: Add and run the fork-inheritance test**

Add to `EmbedSpawnerForkTest.java`:

```java
    @Test
    public void testForkInheritsMaxEmbedDepth() {
        final TikaDocument root = new DocumentFactory()
                .withIdentifier(new DigestIdentifier("SHA-256", Charset.defaultCharset()))
                .create(Paths.get("/tmp/fake-root.ost"));
        final EmbedSpawner base = new EmbedSpawner(root, new ParseContext(), null,
                w -> new org.apache.tika.sax.BodyContentHandler(w),
                64L * 1024 * 1024, new TemporaryResources(), () -> false, 5);
        final EmbedSpawner forked = base.fork();
        assertThat(forked.maxEmbedDepthForTest()).isEqualTo(5);
    }
```

Run: `mvn -pl extract-lib -am test -Dtest=EmbedSpawnerForkTest`
Expected: PASS (existing fork tests plus the new one).

- [ ] **Step 7: Commit**

```bash
git add extract-lib/src/main/java/org/icij/extract/extractor/EmbedSpawner.java \
        extract-lib/src/test/java/org/icij/extract/extractor/EmbedSpawnerDepthGuardTest.java \
        extract-lib/src/test/java/org/icij/extract/extractor/EmbedSpawnerForkTest.java
git commit -m "feat(extractor): refuse and record embeds nested beyond max depth"
```

---

### Task 4: Expose maxEmbedDepth on Extractor

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/extract/extractor/Extractor.java`
- Test: `extract-lib/src/test/java/org/icij/extract/extractor/ExtractorTest.java` (modify if present; otherwise create)

**Interfaces:**
- Consumes: `EmbedSpawner` master constructor's new trailing `int maxEmbedDepth` parameter (Task 3).
- Produces: `int Extractor.getMaxEmbedDepth()`; option `maxEmbedDepth` parsed from `Options`.

- [ ] **Step 1: Write the failing test**

If `ExtractorTest.java` exists, add these tests; otherwise create the file with this skeleton (adjust package imports to match a nearby Extractor test):

```java
    @Test public void testMaxEmbedDepthDefaultsTo20() {
        Extractor extractor = new Extractor(Options.from(java.util.Map.of()));
        assertThat(extractor.getMaxEmbedDepth()).isEqualTo(20);
    }

    @Test public void testMaxEmbedDepthReadsOption() {
        Extractor extractor = new Extractor(Options.from(java.util.Map.of("maxEmbedDepth", "8")));
        assertThat(extractor.getMaxEmbedDepth()).isEqualTo(8);
    }

    @Test public void testMaxEmbedDepthZeroDisables() {
        Extractor extractor = new Extractor(Options.from(java.util.Map.of("maxEmbedDepth", "0")));
        assertThat(extractor.getMaxEmbedDepth()).isEqualTo(0);
    }

    @Test public void testMaxEmbedDepthClampsNegativeToZero() {
        Extractor extractor = new Extractor(Options.from(java.util.Map.of("maxEmbedDepth", "-3")));
        assertThat(extractor.getMaxEmbedDepth()).isEqualTo(0);
    }
```

Required imports for these tests (add if missing): `import org.icij.task.Options;` and `import static org.fest.assertions.Assertions.assertThat;`.

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl extract-lib -am test -Dtest=ExtractorTest`
Expected: FAIL — compilation error, `getMaxEmbedDepth()` undefined.

- [ ] **Step 3: Add the option annotation, field, parse, and getter**

In `Extractor.java`:

a) Add an `@Option` annotation with the other class-level annotations (after the `legacyUntitledNaming` option, around line 149):

```java
@Option(name = "maxEmbedDepth", description = "Maximum nesting depth of embedded documents to " +
        "descend into. Embeds deeper than this are skipped (recorded, not parsed) to guard against " +
        "decompression-bomb archives. Defaults to 20. Set to 0 to disable.", parameter = "count")
```

b) Add the field with the other option fields (after `legacyUntitledNaming`, around line 212):

```java
    private int maxEmbedDepth = EmbedSpawner.DEFAULT_MAX_EMBED_DEPTH;
```

c) Parse it in `configure()` (after the `legacyUntitledNaming` parse, around line 308), clamping negatives to zero:

```java
        options.get("maxEmbedDepth", String.valueOf(EmbedSpawner.DEFAULT_MAX_EMBED_DEPTH))
                .parse().asInteger().ifPresent(n -> this.maxEmbedDepth = Math.max(0, n));
```

d) Add the getter with the other option getters (near `isLegacyUntitledNaming()`, around line 394):

```java
    public int getMaxEmbedDepth() { return maxEmbedDepth; }
```

- [ ] **Step 4: Wire maxEmbedDepth into the EmbedSpawner construction**

In `getTikaDocument`, the `EmbedSpawner` is built at lines 950-954. Add `maxEmbedDepth` as the final constructor argument. Change:

```java
            context.set(EmbeddedDocumentExtractor.class,
                    new EmbedSpawner(rootDocument, context, embedOutput, handlerProvider, embedMemoryBudgetBytes,
                            embedTextResources, new MemoryPressureGauge(embedMemoryPressureThreshold),
                            this::ocrExecutor, !ocrDisabled, currentProgress, digester,
                            ocrFanout, ocrMinImageBytes, ocrParserClassName, sink, legacyUntitledNaming));
```

to:

```java
            context.set(EmbeddedDocumentExtractor.class,
                    new EmbedSpawner(rootDocument, context, embedOutput, handlerProvider, embedMemoryBudgetBytes,
                            embedTextResources, new MemoryPressureGauge(embedMemoryPressureThreshold),
                            this::ocrExecutor, !ocrDisabled, currentProgress, digester,
                            ocrFanout, ocrMinImageBytes, ocrParserClassName, sink, legacyUntitledNaming,
                            maxEmbedDepth));
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `mvn -pl extract-lib -am test -Dtest=ExtractorTest`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add extract-lib/src/main/java/org/icij/extract/extractor/Extractor.java \
        extract-lib/src/test/java/org/icij/extract/extractor/ExtractorTest.java
git commit -m "feat(extractor): add maxEmbedDepth option (default 20, 0 disables)"
```

---

### Task 5: Surface skipped count in the heartbeat log

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/extract/extractor/LoggingProgressListener.java`
- Test: `extract-lib/src/test/java/org/icij/extract/extractor/LoggingProgressListenerTest.java`

**Interfaces:**
- Consumes: `ExtractionProgress.embedsSkippedMaxDepth()` (Task 2).
- Produces: updated `static String LoggingProgressListener.formatLine(ExtractionProgress, long)`.

- [ ] **Step 1: Write/adjust the failing test**

The existing `testFormatLine` asserts an exact string that the new format changes. Update its expected value to include the new suffix (the counter is 0 in that test since no skip was recorded). Change:

```java
        assertThat(LoggingProgressListener.formatLine(p, 3_000L))
            .isEqualTo("/x.ost: 3s, 2 embeds, 1/3 OCR done");
```

to:

```java
        assertThat(LoggingProgressListener.formatLine(p, 3_000L))
            .isEqualTo("/x.ost: 3s, 2 embeds, 1/3 OCR done, 0 skipped(maxDepth)");
```

Then add a dedicated test that exercises a non-zero skip count:

```java
    @Test public void testFormatLineIncludesSkippedCount() {
        ExtractionProgress p = new ExtractionProgress(Paths.get("/x.ost"), 1_000L);
        p.incrementEmbeds();
        p.incrementEmbedsSkippedMaxDepth();
        assertThat(LoggingProgressListener.formatLine(p, 3_000L)).contains("1 skipped(maxDepth)");
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl extract-lib -am test -Dtest=LoggingProgressListenerTest`
Expected: FAIL — the formatted line does not yet contain `skipped(maxDepth)` (or the existing exact-string test fails against the new format).

- [ ] **Step 3: Update formatLine**

In `LoggingProgressListener.java`, change `formatLine` (lines 17-21) to append the skipped count:

```java
    static String formatLine(final ExtractionProgress p, final long now) {
        final long seconds = p.elapsedMillis(now) / 1000L;
        return String.format("%s: %ds, %d embeds, %d/%d OCR done, %d skipped(maxDepth)",
                p.path(), seconds, p.embedsParsed(), p.ocrCompleted(), p.ocrSubmitted(),
                p.embedsSkippedMaxDepth());
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn -pl extract-lib -am test -Dtest=LoggingProgressListenerTest`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add extract-lib/src/main/java/org/icij/extract/extractor/LoggingProgressListener.java \
        extract-lib/src/test/java/org/icij/extract/extractor/LoggingProgressListenerTest.java
git commit -m "feat(extractor): show max-depth skip count in progress heartbeat"
```

---

### Task 6: Full-module regression run

**Files:** none (verification only).

- [ ] **Step 1: Run the full extract-lib test suite**

Run: `mvn -pl extract-lib -am test`
Expected: BUILD SUCCESS, no regressions. Pay attention to `EmbedSpawnerForkTest`, `EmbedSpawnerSinkTest`, `EmbedSpawnerUntitledTest`, `EmbedSpawnerDeferredOcrTest` (all exercise `EmbedSpawner` constructors touched in Task 3) and any `ExtractorTest` OCR/embed integration tests.

- [ ] **Step 2: Confirm no version bump and clean spec/plan tracking**

Run: `git status && git diff --stat $(git merge-base HEAD master)..HEAD`
Expected: no change to any `VERSION`/`pom.xml` version.

---

## Self-Review

**Spec coverage:**
- Guard dimension = max depth only → Tasks 1, 3. ✓
- Enforcement in `parseEmbedded` non-inline branch, before spool/recurse → Task 3 Step 4. ✓
- Depth semantics `size() > maxEmbedDepth` → Task 1 predicate + Task 3 wiring. ✓
- Refuse (no spool/add/recurse) → Task 3 Step 4 (`return` before `incrementEmbeds`/`spawnEmbedded`). ✓
- Parent metadata marker `X-EXTRACT:embedsSkippedMaxDepth` → Task 3 `recordSkippedAtDepth`. ✓
- Progress counter → Task 2 + Task 3. ✓
- Config `maxEmbedDepth` default 20 / 0 disables / on by default → Task 4. ✓
- Plumbing through constructors + fork + Extractor → Tasks 3, 4. ✓
- Byte-identity no-op for within-limit embeds → guard `return`s only on refuse; verified by disabled-path test (Task 3) and full regression (Task 6). ✓
- Heartbeat visibility → Task 5. ✓
- Tests enumerated in spec → Tasks 1-5 cover trip/allow/disabled/marker+counter/fork/heartbeat; deep synthetic archive covered by disabled-vs-refused unit tests plus full regression (a live nested-zip fixture is not required to prove the branch and is heavier than warranted). ✓

**Placeholder scan:** No TBD/TODO; every code step shows complete code. ✓

**Type consistency:** `exceedsMaxEmbedDepth(int,int)`, `EMBEDS_SKIPPED_MAX_DEPTH`, `DEFAULT_MAX_EMBED_DEPTH`, `incrementEmbedsSkippedMaxDepth()`/`embedsSkippedMaxDepth()`, `getMaxEmbedDepth()`, and `maxEmbedDepthForTest()` are used consistently across tasks. Master constructor gains exactly one trailing `int maxEmbedDepth`; every delegating constructor and the `Extractor` call site pass it. ✓

**Note for the implementer:** If `ExtractorTest.java` does not exist, place the Task 4 tests in the nearest existing Extractor-level test class and match its package/imports; the assertions themselves are self-contained.
