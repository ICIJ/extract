# OST/PST folder fan-out — code-review remediation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the critical fork-isolation byte-identity bug and the lesser concurrency/correctness issues a fresh `/code-review max` found on `feat/pst-folder-fanout`, so the branch is genuinely merge-ready.

**Architecture:** Each folder-walk fork gets its OWN `ParseContext` registering the fork itself as the `EmbeddedDocumentExtractor` (so nested embeds recurse into the fork's own DFS stack, not the shared base spawner) and deliberately OMITTING `PstFanoutConfig` (so nested PSTs walk serially — no reentrant pool starvation). A `legacyUntitledNaming` flag restores the pre-branch global `untitled_N` scheme for on-demand dual-resolution of old corpora. Thread-safety hardening on the now-shared `TikaDocument` root and the parallel-walk cleanup path.

**Tech Stack:** Java 21, Maven, Tika 3.3.0, java-libpst (`com.pff`), JUnit 4 + fest-assert. Build/test in the extract repo.

## Global Constraints

- Branch `feat/pst-folder-fanout` off `master` (extract default branch); ff/rebase, no squash; commit via `/commit`; NO Claude attribution (no Co-Authored-By / "Generated with" trailers).
- **Do NOT bump `extract` version on the feature branch** — versioning happens at release. Pom stays 9.12.0.
- Byte-identity invariant is sacred: embed ids / `X-TIKA:digest:*` / artifact filenames / `OCR_PARSER` / `PST_FOLDER_PATH` must be identical between fan-out and serial modes. Production uses `DigestIdentifier` (SHA-384): `generateForEmbed = digest(own-content-hash + parent.getId() + embeddedRelationshipId + name)`.
- Full extract-lib suite must stay green except the two known pre-existing native-Tesseract failures (`Tess4JOCRParserTest`, `ExtractorTest.test_ocr_confidence`).
- No em dashes in prose written into code comments/docs.
- extract single test: `mvn -pl extract-lib test -Dtest=ClassName -DfailIfNoTests=false`
- extract full suite: `mvn -pl extract-lib test`
- Spec: `docs/superpowers/specs/2026-07-01-ost-folder-fanout-remediation-design.md`.

---

### Task 1: CRITICAL — fork-owned ParseContext isolates nested embeds (drives out the byte-identity bug)

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/extract/extractor/EmbedSpawner.java` (copy-ctor ~147; `buildIsolatedOcrContext` ~499)
- Rewrite test: `extract-lib/src/test/java/org/icij/extract/extractor/PstFolderFanoutDeterminismTest.java`

**Interfaces:**
- Consumes: `EmbedSpawner.fork()` (existing), `Extractor(Options)` (existing), `DigestIdentifier` / `UpdatableDigester` (existing, see `DatashareExtractIntegrationTest.createExtractor` pattern).
- Produces: fork instances whose nested parses recurse into the fork itself. No new public API.

**Context:** The fork copy-ctor does `super(template.root, template.context)` — sharing the base context. `delegateParsing` runs nested parses against `this.context`, whose registered `EmbeddedDocumentExtractor` is the BASE spawner, so message attachments (depth>=2) are handled by the base spawner from all threads (data race + parent=root instead of parent=message → wrong digest id). Fix: fork owns a cloned context registering itself as the extractor, omitting `PstFanoutConfig`.

- [ ] **Step 1: Rewrite the guardian determinism test to use DigestIdentifier + the full identity tuple**

Replace the body of `PstFolderFanoutDeterminismTest.java` with:

```java
package org.icij.extract.extractor;

import org.apache.tika.metadata.TikaCoreProperties;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.EmbeddedTikaDocument;
import org.icij.extract.document.TikaDocument;
import org.icij.spewer.Spewer;
import org.icij.task.Options;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.Map;
import java.util.TreeSet;

import static org.fest.assertions.Assertions.assertThat;

/**
 * Sacred-invariant guard. A PST parsed with folder fan-out ON must produce the SAME SET and the
 * SAME COUNT of {embed id, X-TIKA:digest:SHA384, resource-name, PST_FOLDER_PATH, OCR_PARSER} as the
 * serial walk. Driven with the PRODUCTION DigestIdentifier (SHA-384): the embed id derives from
 * parent.getId(), so the fork-isolation bug (attachment attributed to root instead of its message)
 * changes the id and this test fails. (The previous version used the default path-based identifier,
 * which is why it missed the bug.) OCR is disabled so the test does not depend on Tesseract.
 */
public class PstFolderFanoutDeterminismTest {

    private static final String PST = "/documents/pst/testPST.pst";

    private DocumentFactory digestFactory() {
        // Mirror DatashareExtractIntegrationTest.createExtractor: SHA-384 digest id, resolvable
        // on-demand, so the id depends on parent.getId() + name (the fork-sensitive inputs).
        return new DocumentFactory().withIdentifier(
                new org.icij.extract.document.DigestIdentifier("SHA-384", java.nio.charset.StandardCharsets.UTF_8));
    }

    private TreeSet<String> identitySet(final boolean fanout) throws Exception {
        final DocumentFactory factory = digestFactory();
        try (Extractor extractor = new Extractor(factory, Options.from(Map.of(
                "pstFolderFanout", String.valueOf(fanout),
                "pstParseParallelism", "8",
                "ocr", "false",
                "digestAlgorithm", "SHA-384")))) {
            final TikaDocument doc = extractor.extract(Paths.get(getClass().getResource(PST).toURI()));
            try (java.io.Reader r = doc.getReader()) { Spewer.toString(r); }
            final TreeSet<String> ids = new TreeSet<>();
            collect(doc, ids);
            return ids;
        }
    }

    private void collect(final TikaDocument doc, final TreeSet<String> ids) throws Exception {
        for (final EmbeddedTikaDocument embed : doc.getEmbeds()) {
            ids.add(embed.getId() + "|"
                    + embed.getMetadata().get("X-TIKA:digest:SHA384") + "|"
                    + embed.getMetadata().get(TikaCoreProperties.RESOURCE_NAME_KEY) + "|"
                    + embed.getMetadata().get(org.apache.tika.metadata.PST.PST_FOLDER_PATH) + "|"
                    + embed.getMetadata().get(org.icij.extract.ocr.OCRParser.OCR_PARSER));
            collect(embed, ids);
        }
    }

    private int count(final TikaDocument doc) {
        int n = 0;
        for (final EmbeddedTikaDocument embed : doc.getEmbeds()) {
            n += 1 + count(embed);
        }
        return n;
    }

    @Test(timeout = 180_000)
    public void testFanoutIdentityMatchesSerialAcrossRepeatedRuns() throws Exception {
        final TreeSet<String> serial = identitySet(false);
        assertThat(serial).isNotEmpty();
        // testPST.pst must be genuinely multi-folder so fan-out actually runs (see Task 5 in the
        // original plan): its 5 folders drive walkFoldersParallel.
        for (int run = 0; run < 5; run++) {
            final TreeSet<String> fanout = identitySet(true);
            assertThat(fanout).as("identity set, run " + run).isEqualTo(serial);
        }
    }

    @Test(timeout = 180_000)
    public void testFanoutEmbedCountMatchesSerial() throws Exception {
        final DocumentFactory factory = digestFactory();
        int serialCount;
        try (Extractor extractor = new Extractor(factory, Options.from(Map.of(
                "pstFolderFanout", "false", "ocr", "false", "digestAlgorithm", "SHA-384")))) {
            final TikaDocument doc = extractor.extract(Paths.get(getClass().getResource(PST).toURI()));
            try (java.io.Reader r = doc.getReader()) { Spewer.toString(r); }
            serialCount = count(doc);
        }
        assertThat(serialCount).isGreaterThan(1);
        try (Extractor extractor = new Extractor(digestFactory(), Options.from(Map.of(
                "pstFolderFanout", "true", "pstParseParallelism", "8", "ocr", "false",
                "digestAlgorithm", "SHA-384")))) {
            final TikaDocument doc = extractor.extract(Paths.get(getClass().getResource(PST).toURI()));
            try (java.io.Reader r = doc.getReader()) { Spewer.toString(r); }
            assertThat(count(doc)).isEqualTo(serialCount);
        }
    }
}
```

NOTE for the implementer: confirm the exact `Extractor` constructor that accepts `(DocumentFactory, Options)` and the `DigestIdentifier` constructor signature by reading `DatashareExtractIntegrationTest.createExtractor` and `ParallelOcrDeterminismTest`; match them exactly (the digest metadata key may be `X-TIKA:digest:SHA384` — verify against an existing digest test and adjust the string if the project uses a different casing). Do NOT invent an API.

- [ ] **Step 2: Run the guardian test to verify it FAILS on current code (proves the bug)**

Run: `mvn -pl extract-lib test -Dtest=PstFolderFanoutDeterminismTest -DfailIfNoTests=false`
Expected: FAIL — fan-out identity set / count differs from serial because attachments are attributed to root under the shared base spawner. (If it unexpectedly PASSES, STOP and investigate: the tuple or identifier is wrong and is not exercising `parent.getId()`.)

- [ ] **Step 3: Implement the fork-owned context**

In `EmbedSpawner.java`, factor the parsing-config copy out of `buildIsolatedOcrContext` into a shared helper, and change the copy-ctor to build + own a fork context:

Add the helper (near `buildIsolatedOcrContext`):

```java
// Copies the parsing collaborators every derived parse needs (parser + OCR/PDF/HTML config +
// selector) from `source` into `target`, WITHOUT the EmbeddedDocumentExtractor or PstFanoutConfig.
// Single maintenance point shared by fork() and buildIsolatedOcrContext(): if Extractor starts
// setting a new parsing-config key on the context, add it here once.
private static void copyParsingConfig(final ParseContext source, final ParseContext target) {
    final Parser p = source.get(Parser.class);
    if (p != null) { target.set(Parser.class, p); }
    final TesseractOCRConfig tess = source.get(TesseractOCRConfig.class);
    if (tess != null) { target.set(TesseractOCRConfig.class, tess); }
    final PDFParserConfig pdf = source.get(PDFParserConfig.class);
    if (pdf != null) { target.set(PDFParserConfig.class, pdf); }
    final HtmlMapper html = source.get(HtmlMapper.class);
    if (html != null) { target.set(HtmlMapper.class, html); }
    final DocumentSelector selector = source.get(DocumentSelector.class);
    if (selector != null) { target.set(DocumentSelector.class, selector); }
}

// Builds a fork's own ParseContext: a copy of the base parsing config, MINUS the
// EmbeddedDocumentExtractor (the caller registers `this` after super() so nested embeds recurse
// into THIS fork's own DFS stack instead of the shared base spawner -- the byte-identity fix) and
// MINUS PstFanoutConfig (so a nested PST/OST attachment reads null -> canFanOut=false -> serial
// walk, so only the outermost PST fans out; no reentrant pstParseExecutor starvation).
private static ParseContext forkContext(final ParseContext base) {
    final ParseContext fork = new ParseContext();
    copyParsingConfig(base, fork);
    return fork;
}
```

Refactor `buildIsolatedOcrContext` to reuse the helper (behavior unchanged):

```java
private ParseContext buildIsolatedOcrContext() {
    final ParseContext isolated = new ParseContext();
    copyParsingConfig(context, isolated);
    // Ignore any nested embed discovered inside the image rather than mutating shared state.
    isolated.set(EmbeddedDocumentExtractor.class, new EmbedBlocker());
    return isolated;
}
```

Change the copy-ctor (lines ~147-164) to pass the fork context to super and register self:

```java
private EmbedSpawner(final EmbedSpawner template) {
    super(template.root, forkContext(template.context));
    this.outputPath = template.outputPath;
    this.handlerFunction = template.handlerFunction;
    this.embedMemoryBudgetBytes = template.embedMemoryBudgetBytes;
    this.tmp = template.tmp;
    this.memoryPressureHigh = template.memoryPressureHigh;
    this.ocrExecutorSupplier = template.ocrExecutorSupplier;
    this.ocrEnabled = template.ocrEnabled;
    this.progress = template.progress;
    this.digester = template.digester;
    this.ocrFanout = template.ocrFanout;
    this.ocrMinImageBytes = template.ocrMinImageBytes;
    this.ocrParserClassName = template.ocrParserClassName;
    this.sink = template.sink;
    this.reserved = template.reserved; // SHARED budget
    // Register THIS fork as the extractor in its OWN context so nested embeds (message attachments,
    // depth>=2) recurse into this fork's DFS stack + untitled map, not the shared base spawner.
    this.context.set(EmbeddedDocumentExtractor.class, this);
    tikaDocumentStack.add(template.root); // fresh stack, seeded with the shared root
}
```

Add the imports if not already present (they are, for buildIsolatedOcrContext): `Parser`, `TesseractOCRConfig`, `PDFParserConfig`, `HtmlMapper`, `DocumentSelector`, `EmbeddedDocumentExtractor`.

- [ ] **Step 4: Run the guardian test to verify it PASSES**

Run: `mvn -pl extract-lib test -Dtest=PstFolderFanoutDeterminismTest -DfailIfNoTests=false`
Expected: PASS (both tests, all 5 repeats). If `PST_FOLDER_PATH` is the only differing component, reconcile: fan-out already uses `PstFolderPathResolver.folderPaths` (documented to produce strings identical to the serial walk); a divergence means a folder-name edge case — read `PstFolderPathResolver.buildPath` vs `ResilientOutlookPSTParser.childFolderPath` and align. Do not weaken the tuple.

- [ ] **Step 5: Run serial-path regressions to prove serial mode is unchanged**

Run: `mvn -pl extract-lib test -Dtest='ParallelOcrDeterminismTest,StreamingSpewPstStartupTest,ResilientOutlookPSTParserTest,EmbedSpawnerForkTest,EmbedSpawnerSinkTest' -DfailIfNoTests=false`
Expected: PASS (all). Serial mode registers one spawner in the context and is behaviorally identical.

- [ ] **Step 6: Commit**

```bash
git add extract-lib/src/main/java/org/icij/extract/extractor/EmbedSpawner.java \
        extract-lib/src/test/java/org/icij/extract/extractor/PstFolderFanoutDeterminismTest.java
git commit -m "fix(pst): isolate nested embeds in forked spawners via a fork-owned parse context"
```

---

### Task 2: Guard — fork context registers self and omits PstFanoutConfig (proves #2 fix)

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/extract/extractor/EmbedSpawner.java` (add one test accessor)
- Modify test: `extract-lib/src/test/java/org/icij/extract/extractor/EmbedSpawnerForkTest.java`

**Interfaces:**
- Produces: package-private `ParseContext EmbedSpawner.parseContextForTest()` returning `this.context`.

**Context:** #2 (reentrant pool starvation) is subsumed by Task 1: because the fork context omits `PstFanoutConfig`, a nested PST/OST parsed inside a fork reads `null` and walks serially. A real PST-in-PST behavioral fixture cannot be committed (authoring nested-PST test data needs Outlook; java-libpst is read-only — same fixture limitation the OST-2013 work documented). This deterministic unit test proves the mechanism that prevents reentrant fan-out instead.

- [ ] **Step 1: Add the test accessor**

In `EmbedSpawner.java`, next to the other test accessors (`reservedBudget()`, `stackDepth()`):

```java
// Test accessor (package-private): the context this spawner runs nested parses against.
ParseContext parseContextForTest() { return context; }
```

- [ ] **Step 2: Write the failing test**

Append to `EmbedSpawnerForkTest.java`:

```java
    @Test
    public void testForkContextRegistersSelfAndOmitsFanout() {
        final org.apache.tika.parser.ParseContext base = new org.apache.tika.parser.ParseContext();
        // The base context carries the fan-out config (this is what makes the OUTERMOST PST fan out).
        base.set(org.icij.extract.parser.PstFanoutConfig.class,
                new org.icij.extract.parser.PstFanoutConfig(true, () -> null));
        final org.icij.extract.document.TikaDocument root =
                new org.icij.extract.document.DocumentFactory()
                        .withIdentifier(new org.icij.extract.document.DigestIdentifier("SHA-384",
                                java.nio.charset.StandardCharsets.UTF_8))
                        .create(java.nio.file.Paths.get("/tmp/fake-root.ost"));
        final EmbedSpawner baseSpawner = new EmbedSpawner(root, base, null,
                w -> new org.apache.tika.sax.BodyContentHandler(w),
                64L * 1024 * 1024, new org.apache.tika.io.TemporaryResources(), () -> false);
        final EmbedSpawner forked = baseSpawner.fork();

        // The fork registers ITSELF as the extractor so nested embeds recurse into the fork.
        assertThat(forked.parseContextForTest()
                .get(org.apache.tika.extractor.EmbeddedDocumentExtractor.class)).isSameAs(forked);
        // The fork context OMITS PstFanoutConfig, so a nested PST attachment walks serially
        // (no reentrant pstParseExecutor starvation).
        assertThat(forked.parseContextForTest()
                .get(org.icij.extract.parser.PstFanoutConfig.class)).isNull();
    }
```

- [ ] **Step 3: Run to verify it passes** (accessor + Task 1 fix already make it green)

Run: `mvn -pl extract-lib test -Dtest=EmbedSpawnerForkTest -DfailIfNoTests=false`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add extract-lib/src/main/java/org/icij/extract/extractor/EmbedSpawner.java \
        extract-lib/src/test/java/org/icij/extract/extractor/EmbedSpawnerForkTest.java
git commit -m "test(pst): assert fork context self-registers and omits fan-out (nested PST stays serial)"
```

---

### Task 3: `legacyUntitledNaming` flag restores the global untitled scheme (dual-resolution, extract-lib side)

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/extract/extractor/Extractor.java` (options block + parsing + getter + pass to spawner ctor)
- Modify: `extract-lib/src/main/java/org/icij/extract/extractor/EmbedSpawner.java` (field + DRY the two naming sites into one method + restore global counter)
- Test: `extract-lib/src/test/java/org/icij/extract/extractor/EmbedSpawnerUntitledTest.java` (extend)

**Interfaces:**
- Consumes: existing `untitledName(String, int)`.
- Produces: `boolean Extractor.isLegacyUntitledNaming()`; package-private static `String EmbedSpawner.legacyUntitledName(int n)`; a 16th ctor arg `legacyUntitledNaming` on the primary constructor; a private `String nextUntitledName()` that both naming sites call.

**Context:** The per-parent scheme (fan-out-deterministic) changed serial-mode naming vs the pre-branch release, so on-demand download of OLD corpora recomputes a different id. `legacyUntitledNaming` (default false) restores the pre-branch global `untitled_N` counter for datashare's on-demand fallback re-walk (Task 7, gated). Per-parent stays the default primary. Legacy mode is meaningful only in serial mode (global counter deterministic only without fan-out), which is where on-demand resolution runs.

- [ ] **Step 1: Write the failing tests**

Append to `EmbedSpawnerUntitledTest.java`:

```java
    @Test
    public void testLegacyUntitledNameMatchesPreBranchFormat() {
        // Pre-branch: String.format("untitled_%d", ++untitled) -> untitled_1, untitled_2, ...
        assertThat(EmbedSpawner.legacyUntitledName(1)).isEqualTo("untitled_1");
        assertThat(EmbedSpawner.legacyUntitledName(2)).isEqualTo("untitled_2");
    }
```

Create `extract-lib/src/test/java/org/icij/extract/extractor/LegacyUntitledNamingOptionTest.java`:

```java
package org.icij.extract.extractor;

import org.icij.task.Options;
import org.junit.Test;

import java.util.Map;

import static org.fest.assertions.Assertions.assertThat;

public class LegacyUntitledNamingOptionTest {

    @Test
    public void testDefaultsFalse() throws Exception {
        try (Extractor extractor = new Extractor(Options.from(Map.of()))) {
            assertThat(extractor.isLegacyUntitledNaming()).isFalse();
        }
    }

    @Test
    public void testOverrideTrue() throws Exception {
        try (Extractor extractor = new Extractor(Options.from(Map.of("legacyUntitledNaming", "true")))) {
            assertThat(extractor.isLegacyUntitledNaming()).isTrue();
        }
    }
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `mvn -pl extract-lib test -Dtest=EmbedSpawnerUntitledTest,LegacyUntitledNamingOptionTest -DfailIfNoTests=false`
Expected: FAIL — `legacyUntitledName` and `isLegacyUntitledNaming` do not exist (compile error).

- [ ] **Step 3: Implement**

In `EmbedSpawner.java`:

Add the field next to `untitledOrdinalsByParent`:

```java
// When true, nameless non-inline embeds use the pre-branch GLOBAL untitled_N counter instead of the
// per-parent scheme, so datashare's on-demand SourceExtractor can resolve embeds in corpora indexed
// before the per-parent change (dual-resolution). Meaningful only in serial mode: the global counter
// is deterministic only without fan-out, which is where on-demand resolution runs.
private final boolean legacyUntitledNaming;
// Pre-branch global counter, used only when legacyUntitledNaming is true.
private int untitledGlobalCounter = 0;
```

Add the static legacy formatter next to `untitledName`:

```java
// Pre-branch format: String.format("untitled_%d", ++untitled). n is 1-based.
static String legacyUntitledName(final int n) {
    return "untitled_" + n;
}
```

Add a single naming method and call it from both sites:

```java
// One place that decides a nameless non-inline embed's name, so both spawn paths agree.
private String nextUntitledName() {
    if (legacyUntitledNaming) {
        return legacyUntitledName(++untitledGlobalCounter);
    }
    final String parentId = tikaDocumentStack.getLast().getId();
    final int ordinal = untitledOrdinalsByParent.merge(parentId, 1, Integer::sum) - 1;
    return untitledName(parentId, ordinal);
}
```

Replace BOTH naming blocks (in `spawnEmbedded` ~250-255 and `spawnEmbeddedDeferred` ~320-325):

```java
String name = metadata.get(TikaCoreProperties.RESOURCE_NAME_KEY);
if (null == name || name.isEmpty()) {
    name = nextUntitledName();
}
```

Thread `legacyUntitledNaming` through the constructors. In the 16-arg primary constructor (currently 15 args ending in `sink`), add a final `boolean legacyUntitledNaming` parameter and `this.legacyUntitledNaming = legacyUntitledNaming;`. In the two convenience constructors (the 7-arg serial one and the 13-arg backward-compat overload) that delegate via `this(...)`, pass `false` for the new argument. In the copy-ctor, add `this.legacyUntitledNaming = template.legacyUntitledNaming;`.

In `Extractor.java`:
- Add to the `@Option` block (near the pstFolderFanout option):

```java
@Option(name = "legacyUntitledNaming", description = "Name nameless non-inline embeds with the " +
        "pre-9.x global untitled_N counter instead of the per-parent scheme, for on-demand " +
        "resolution of corpora indexed before the per-parent change. Serial mode only.")
```

- Add the field default (near `pstFolderFanout`): `private boolean legacyUntitledNaming = false;`
- Add option parsing (near the pstFolderFanout parse): `options.get("legacyUntitledNaming", "false").parse().asBoolean().ifPresent(b -> this.legacyUntitledNaming = b);`
- Add getter: `public boolean isLegacyUntitledNaming() { return legacyUntitledNaming; }`
- Pass `legacyUntitledNaming` as the final argument to the `new EmbedSpawner(...)` call at ~936.

- [ ] **Step 4: Run to verify they pass, plus determinism unaffected**

Run: `mvn -pl extract-lib test -Dtest='EmbedSpawnerUntitledTest,LegacyUntitledNamingOptionTest,PstFolderFanoutDeterminismTest,ParallelOcrDeterminismTest' -DfailIfNoTests=false`
Expected: PASS (all). Default (per-parent) is unchanged, so the determinism guard still holds.

- [ ] **Step 5: Commit**

```bash
git add extract-lib/src/main/java/org/icij/extract/extractor/Extractor.java \
        extract-lib/src/main/java/org/icij/extract/extractor/EmbedSpawner.java \
        extract-lib/src/test/java/org/icij/extract/extractor/EmbedSpawnerUntitledTest.java \
        extract-lib/src/test/java/org/icij/extract/extractor/LegacyUntitledNamingOptionTest.java
git commit -m "feat(pst): add legacyUntitledNaming flag restoring the global untitled scheme for on-demand dual-resolution"
```

---

### Task 4: Arm PstStdoutFilter suppression inside each folder-walk task

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/extract/parser/ResilientOutlookPSTParser.java` (`walkSingleFolderParallel`)
- Modify: `extract-lib/src/main/java/org/icij/extract/cli/PstStdoutFilter.java` (add a test accessor if none exists)
- Test: `extract-lib/src/test/java/org/icij/extract/parser/PstStdoutFilterFanoutTest.java` (create)

**Interfaces:**
- Consumes: `PstStdoutFilter.runWithSuppression(Runnable)` OR `begin()`/`end()` (read the class to use its existing depth-counter lifecycle — the F4/Wave-B `runWithSuppressionLifted` sibling).
- Produces: suppression armed on `extract-pst-walk-*` threads; package-private `int PstStdoutFilter.depthForTest()` if no read accessor exists.

**Context:** libpst stdout noise suppression is a ThreadLocal armed only on the parse thread; fan-out moves libpst work to pool threads where it is inactive. Arm the existing depth-counter suppression around each folder task's body.

- [ ] **Step 1: Read `PstStdoutFilter` to learn its exact lifecycle API**

Read `extract-lib/src/main/java/org/icij/extract/cli/PstStdoutFilter.java`. It has a depth-counter `begin()`/`end()` (Wave B, commit 3f807e9) and `install()`. Determine the read accessor for the current thread's depth; if none, add:

```java
// Test accessor (package-private): the current thread's suppression nesting depth.
static int depthForTest() { /* return the ThreadLocal counter's current value, 0 if unset */ }
```

(Implement to match the existing ThreadLocal field; e.g. `return DEPTH.get();` if `DEPTH` is an `Integer` ThreadLocal initialised to 0.)

- [ ] **Step 2: Write the failing test**

Create `PstStdoutFilterFanoutTest.java` (in package `org.icij.extract.parser` to reach the parser; import the filter):

```java
package org.icij.extract.parser;

import org.icij.extract.cli.PstStdoutFilter;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

public class PstStdoutFilterFanoutTest {

    @Test
    public void testSuppressionArmedInsideFolderTaskBody() {
        PstStdoutFilter.install();
        // Simulate a pool thread with no suppression armed at entry.
        assertThat(PstStdoutFilter.depthForTest()).isEqualTo(0);
        PstStdoutFilter.runWithSuppression(() ->
                assertThat(PstStdoutFilter.depthForTest()).isGreaterThan(0));
        // Balanced: depth returns to 0 after the task body.
        assertThat(PstStdoutFilter.depthForTest()).isEqualTo(0);
    }
}
```

If the filter exposes `begin()`/`end()` rather than a `runWithSuppression(Runnable)` wrapper, add a small `static void runWithSuppression(Runnable r) { begin(); try { r.run(); } finally { end(); } }` to `PstStdoutFilter` and test that instead. Use the existing wrapper if one already exists (mirror `runWithSuppressionLifted`).

- [ ] **Step 3: Run to verify it fails**

Run: `mvn -pl extract-lib test -Dtest=PstStdoutFilterFanoutTest -DfailIfNoTests=false`
Expected: FAIL — `depthForTest` / `runWithSuppression` missing (compile error), or depth not incremented.

- [ ] **Step 4: Implement — wrap the folder task body**

In `ResilientOutlookPSTParser.walkSingleFolderParallel`, wrap the existing body in the suppression lifecycle so libpst noise emitted on the pool thread is suppressed:

```java
private void walkSingleFolderParallel(final String pstPath, final int descriptorId,
                                      final String folderPath, final EmissionContext baseEmission,
                                      final Reconciliation reconciliation, final Map<Thread, PSTFile> handles) {
    PstStdoutFilter.runWithSuppression(() -> {
        try {
            final PSTFile own = getOrOpenPstHandle(pstPath, handles);
            if (own == null) {
                return;
            }
            final PSTObject object = PSTObject.detectAndLoadPSTObject(own, (long) descriptorId);
            if (object instanceof PSTFolder) {
                emitFolderParallel((PSTFolder) object, folderPath, baseEmission, reconciliation);
            }
        } catch (final Exception | LinkageError e) {
            logger.warn("PST folder \"{}\" (descriptor {}) parallel walk failed; skipping.",
                    folderPath, descriptorId, e);
        }
    });
}
```

Add the import: `import org.icij.extract.cli.PstStdoutFilter;`. (If `runWithSuppression(Runnable)` does not exist, add it to `PstStdoutFilter` as in Step 2.)

- [ ] **Step 5: Run the new test + the existing filter tests**

Run: `mvn -pl extract-lib test -Dtest='PstStdoutFilterFanoutTest,PstStdoutFilterTest,ResilientOutlookPSTParserTest,PstFolderFanoutDeterminismTest' -DfailIfNoTests=false`
Expected: PASS (all).

- [ ] **Step 6: Commit**

```bash
git add extract-lib/src/main/java/org/icij/extract/parser/ResilientOutlookPSTParser.java \
        extract-lib/src/main/java/org/icij/extract/cli/PstStdoutFilter.java \
        extract-lib/src/test/java/org/icij/extract/parser/PstStdoutFilterFanoutTest.java
git commit -m "fix(pst): arm libpst stdout suppression on fan-out pool threads"
```

---

### Task 5: Await folder-task termination before closing per-thread PSTFile handles

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/extract/parser/ResilientOutlookPSTParser.java` (`cleanupParallelWalk`, `awaitAllTasks`)
- Test: `extract-lib/src/test/java/org/icij/extract/parser/ParallelWalkCleanupTest.java` (create)

**Interfaces:**
- Produces: `cleanupParallelWalk` closes handles only after every task has actually stopped (returned or finished cancelling). New private `awaitQuietly(List<Future<?>>)` that waits for completion swallowing cancellation/execution exceptions.

**Context:** `cleanupParallelWalk` does `cancelFutures` (interrupt) then immediately `closeHandles`. On the abnormal/interrupt path a per-thread `PSTFile` can be closed while a task is still reading it → that folder's remaining messages silently dropped. Await termination first.

- [ ] **Step 1: Write the failing test (handle not closed while a task is still running)**

Create `ParallelWalkCleanupTest.java`:

```java
package org.icij.extract.parser;

import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.fest.assertions.Assertions.assertThat;

/**
 * cleanupParallelWalk must not close a handle a task is still using. We drive the cleanup ordering
 * directly: a task holds a "reading" flag until released; cleanup must wait for the task to finish
 * before the close step observes it. Uses a package-private hook (awaitQuietly) exercised via a
 * small test-visible entry point on the parser.
 */
public class ParallelWalkCleanupTest {

    @Test(timeout = 30_000)
    public void testCleanupAwaitsRunningTaskBeforeClosing() throws Exception {
        final ExecutorService pool = Executors.newFixedThreadPool(1);
        final CountDownLatch started = new CountDownLatch(1);
        final AtomicBoolean stillReading = new AtomicBoolean(true);
        final CountDownLatch release = new CountDownLatch(1);
        final Future<?> f = pool.submit(() -> {
            started.countDown();
            try { release.await(); } catch (final InterruptedException e) { Thread.currentThread().interrupt(); }
            stillReading.set(false);
        });
        started.await();
        // Release shortly, then await: after awaitQuietly returns the task must have finished.
        new Thread(() -> { try { Thread.sleep(200); } catch (final InterruptedException ignored) {} release.countDown(); }).start();
        ResilientOutlookPSTParser.awaitQuietlyForTest(List.of(f));
        assertThat(stillReading.get()).isFalse();
        pool.shutdownNow();
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `mvn -pl extract-lib test -Dtest=ParallelWalkCleanupTest -DfailIfNoTests=false`
Expected: FAIL — `awaitQuietlyForTest` does not exist (compile error).

- [ ] **Step 3: Implement — await termination in cleanup**

In `ResilientOutlookPSTParser.java`, add `awaitQuietly` and reorder cleanup:

```java
private void cleanupParallelWalk(final List<Future<?>> futures, final Map<Thread, PSTFile> handles) {
    cancelFutures(futures);
    // Wait for every task to actually stop before closing handles: a per-thread PSTFile must never
    // be closed while a folder task is still reading it, or that folder's remaining messages are
    // silently dropped on the interrupt/abnormal path.
    awaitQuietly(futures);
    closeHandles(handles);
}

// Waits for each future to complete, swallowing cancellation and execution failures (already logged
// by awaitAllTasks on the normal path). Re-sets the interrupt flag if interrupted while waiting.
private static void awaitQuietly(final List<Future<?>> futures) {
    for (final Future<?> f : futures) {
        try {
            f.get();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        } catch (final java.util.concurrent.CancellationException | java.util.concurrent.ExecutionException ignored) {
            // cancelled or already-failed task: nothing to wait on, nothing actionable here
        }
    }
}

// Test-only entry point for the cleanup await ordering.
static void awaitQuietlyForTest(final List<Future<?>> futures) {
    awaitQuietly(futures);
}
```

- [ ] **Step 4: Run to verify it passes, plus the fan-out suite**

Run: `mvn -pl extract-lib test -Dtest='ParallelWalkCleanupTest,PstFolderFanoutStartupTest,PstFolderFanoutDeterminismTest,ResilientOutlookPSTParserTest' -DfailIfNoTests=false`
Expected: PASS (all).

- [ ] **Step 5: Commit**

```bash
git add extract-lib/src/main/java/org/icij/extract/parser/ResilientOutlookPSTParser.java \
        extract-lib/src/test/java/org/icij/extract/parser/ParallelWalkCleanupTest.java
git commit -m "fix(pst): await folder-task termination before closing PSTFile handles on abort"
```

---

### Task 6: LOW batch — interrupt status, failed-open caching, lazy folder paths, fork stack-isolation test

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/extract/parser/ResilientOutlookPSTParser.java`
- Modify test: `extract-lib/src/test/java/org/icij/extract/extractor/EmbedSpawnerForkTest.java`

**Interfaces:**
- No new public API. `awaitAllTasks` re-sets interrupt; `getOrOpenPstHandle` caches a failed open; `resolveFolderPaths` computed lazily.

- [ ] **Step 1: Fork stack-isolation test — actually push and verify independence**

Replace the existing `testForkSharesBudgetButNotStack` assertions (which only check `stackDepth()==1`) so the test genuinely pushes on the fork. Add a package-private test helper on `EmbedSpawner`:

```java
// Test accessor (package-private): push a document onto this spawner's DFS stack.
void pushForTest(final org.icij.extract.document.TikaDocument doc) { tikaDocumentStack.add(doc); }
```

Then in `EmbedSpawnerForkTest`:

```java
    @Test
    public void testForkStackIsIndependentOfBase() {
        final EmbedSpawner base = newSerialSpawner();
        final EmbedSpawner forked = base.fork();
        assertThat(base.stackDepth()).isEqualTo(1);
        assertThat(forked.stackDepth()).isEqualTo(1);
        forked.pushForTest(new org.icij.extract.document.DocumentFactory()
                .withIdentifier(new org.icij.extract.document.DigestIdentifier("SHA-384",
                        java.nio.charset.StandardCharsets.UTF_8))
                .create(java.nio.file.Paths.get("/tmp/child")));
        assertThat(forked.stackDepth()).isEqualTo(2);
        assertThat(base.stackDepth()).isEqualTo(1); // base is unaffected by the fork's push
    }
```

(Ensure `newSerialSpawner()` uses a DigestIdentifier-backed root, per the Task 3 progress note that `DocumentFactory().create()` NPEs without an identifier.)

- [ ] **Step 2: Run to verify it fails, then add `pushForTest` to make it pass**

Run: `mvn -pl extract-lib test -Dtest=EmbedSpawnerForkTest -DfailIfNoTests=false`
Expected: FAIL first (no `pushForTest`), PASS after adding the accessor.

- [ ] **Step 3: Re-assert interrupt status in `awaitAllTasks`**

In `ResilientOutlookPSTParser.awaitAllTasks`, catch `InterruptedException` and re-set the flag rather than letting the cleared status propagate silently:

```java
private void awaitAllTasks(final List<Future<?>> futures) {
    for (final Future<?> f : futures) {
        try {
            f.get();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return; // stop waiting; cleanupParallelWalk (finally) cancels + drains + closes
        } catch (final ExecutionException e) {
            logger.warn("PST folder task failed.", e.getCause());
        }
    }
}
```

Update its declaration: it no longer needs `throws InterruptedException` (it handles it). Adjust `walkFoldersParallel` accordingly (drop the now-unneeded checked throw if the compiler flags it).

- [ ] **Step 4: Cache a failed per-thread handle open**

In `getOrOpenPstHandle`, `computeIfAbsent` does not store `null`, so a thread whose open failed re-opens on every folder task. Cache the failure with a per-thread sentinel using a marker set of threads that already failed:

```java
private PSTFile getOrOpenPstHandle(final String pstPath, final Map<Thread, PSTFile> handles) {
    final Thread current = Thread.currentThread();
    if (failedHandleThreads.contains(current)) {
        return null; // already tried and failed on this thread; don't reopen per folder
    }
    final PSTFile existing = handles.get(current);
    if (existing != null) {
        return existing;
    }
    final PSTFile opened = openQuietly(pstPath);
    if (opened == null) {
        failedHandleThreads.add(current);
        return null;
    }
    handles.put(current, opened);
    return opened;
}
```

Add the field on the parse-scoped state. Since `handles` is passed per-walk, thread the failure set the same way: add a `final Set<Thread> failedHandleThreads` parameter alongside `handles` (create it in `walkFoldersParallel` as `ConcurrentHashMap.newKeySet()` and pass it through `submitFolderTasks` → `walkSingleFolderParallel` → `getOrOpenPstHandle`). Keep signatures consistent across those four methods.

- [ ] **Step 5: Compute `resolveFolderPaths` lazily**

In `extractAllMessages`, only resolve folder paths when actually needed (the fan-out gate needs it; orphan recovery needs it only when `messageDescriptorIds != null`):

```java
final List<Integer> messageDescriptorIds = enumerateMessageDescriptorIds(pstFile, pstPath);
final boolean fanoutRequested = fanout != null && fanout.enabled()
        && emission.extractor instanceof EmbedSpawner;
final Map<Integer, String> folderPaths =
        (fanoutRequested || messageDescriptorIds != null) ? resolveFolderPaths(pstFile, pstPath) : null;

final boolean canFanOut = fanoutRequested && folderPaths != null && folderPaths.size() > 1;

if (canFanOut) {
    walkFoldersParallel(pstPath, folderPaths, emission, reconciliation, fanout.executor().get());
} else {
    walkFolder(pstFile.getRootFolder(), "/", emission);
}

if (messageDescriptorIds != null) {
    recoverOrphans(messageDescriptorIds, pstFile, folderPaths != null ? folderPaths : resolveFolderPaths(pstFile, pstPath), emission);
}
```

(Preserve the existing behavior: `recoverOrphans` still receives a non-null map. When fan-out did not run but `messageDescriptorIds != null`, `folderPaths` was already resolved by the ternary above, so the second `resolveFolderPaths` call is only a safety net and will rarely execute.)

- [ ] **Step 6: Run the parser + fan-out tests**

Run: `mvn -pl extract-lib test -Dtest='EmbedSpawnerForkTest,ResilientOutlookPSTParserTest,PstFolderFanoutDeterminismTest,PstFolderFanoutStartupTest' -DfailIfNoTests=false`
Expected: PASS (all).

- [ ] **Step 7: Commit**

```bash
git add extract-lib/src/main/java/org/icij/extract/parser/ResilientOutlookPSTParser.java \
        extract-lib/src/main/java/org/icij/extract/extractor/EmbedSpawner.java \
        extract-lib/src/test/java/org/icij/extract/extractor/EmbedSpawnerForkTest.java
git commit -m "fix(pst): batch low-severity fan-out fixes (interrupt status, failed-open cache, lazy folder paths, fork stack test)"
```

---

### Task 7: Harden the shared TikaDocument root — snapshot getEmbeds, safely publish getId

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/extract/document/TikaDocument.java`
- Test: `extract-lib/src/test/java/org/icij/extract/document/TikaDocumentConcurrencyTest.java` (create)

**Interfaces:**
- Produces: `getEmbeds()` returns a snapshot taken under the `embeds` monitor; `getId()` memoization safely published across threads. Behavior for callers is unchanged (same values), only thread-safety.

**Context:** Under fan-out the root `TikaDocument` is shared. `getEmbeds()`/`hasEmbeds()` read the `embeds` LinkedList unsynchronized while `addEmbed`/`removeEmbed` mutate under `synchronized (embeds)` → CME risk if anything iterates during the walk. `getId()` lazily memoizes into a non-volatile `Supplier` field, realized concurrently by many fan-out threads (each attachment's id derivation calls `root.getId()`). Deterministic, but harden the publish.

- [ ] **Step 1: Write the failing/■guard test**

Create `TikaDocumentConcurrencyTest.java`:

```java
package org.icij.extract.document;

import org.apache.tika.metadata.Metadata;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static org.fest.assertions.Assertions.assertThat;

public class TikaDocumentConcurrencyTest {

    private TikaDocument root() {
        return new DocumentFactory()
                .withIdentifier(new DigestIdentifier("SHA-384", StandardCharsets.UTF_8))
                .create(Paths.get("/tmp/root"));
    }

    @Test(timeout = 30_000)
    public void testGetEmbedsSnapshotDoesNotThrowWhileAdding() throws Exception {
        final TikaDocument root = root();
        final ExecutorService pool = Executors.newFixedThreadPool(2);
        final CountDownLatch go = new CountDownLatch(1);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final Runnable adder = () -> { try { go.await(); for (int i = 0; i < 5000; i++) root.addEmbed(new Metadata()); }
                catch (final Throwable t) { failure.set(t); } };
        final Runnable reader = () -> { try { go.await(); for (int i = 0; i < 5000; i++) { for (EmbeddedTikaDocument e : root.getEmbeds()) { e.hashCode(); } } }
                catch (final Throwable t) { failure.set(t); } };
        pool.submit(adder); pool.submit(reader);
        go.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(25, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        assertThat(failure.get()).isNull(); // no ConcurrentModificationException
    }

    @Test(timeout = 30_000)
    public void testGetIdIsStableUnderConcurrentAccess() throws Exception {
        final TikaDocument root = root();
        final ExecutorService pool = Executors.newFixedThreadPool(8);
        final CountDownLatch go = new CountDownLatch(1);
        final List<java.util.concurrent.Future<String>> futures = new java.util.ArrayList<>();
        for (int i = 0; i < 8; i++) {
            futures.add(pool.submit(() -> { go.await(); return root.getId(); }));
        }
        go.countDown();
        final String first = futures.get(0).get();
        for (final java.util.concurrent.Future<String> f : futures) {
            assertThat(f.get()).isEqualTo(first);
        }
        pool.shutdownNow();
    }
}
```

- [ ] **Step 2: Run to verify (the getEmbeds test can throw CME on current code; the getId test guards)**

Run: `mvn -pl extract-lib test -Dtest=TikaDocumentConcurrencyTest -DfailIfNoTests=false`
Expected: the getEmbeds test may FAIL intermittently with CME on current code (unsynchronized read during add); the getId test is a guard. If getEmbeds does not flake, it still must pass after the fix.

- [ ] **Step 3: Implement — snapshot getEmbeds, synchronize getId memoization**

In `TikaDocument.java`, return a snapshot from `getEmbeds()` under the monitor, and make `hasEmbeds()` read under the monitor:

```java
public List<EmbeddedTikaDocument> getEmbeds() {
    synchronized (embeds) {
        return new java.util.ArrayList<>(embeds);
    }
}

public boolean hasEmbeds() {
    synchronized (embeds) {
        return !embeds.isEmpty();
    }
}
```

Make the lazy id memoization safely published. Replace the `Supplier<String> id` self-replacing lambda with a synchronized memoization so concurrent readers on the shared root see a consistent value:

```java
// (field) volatile so the memoized value is safely published across fan-out threads.
private volatile String memoizedId;

public String getId() {
    String value = memoizedId;
    if (value == null) {
        synchronized (this) {
            value = memoizedId;
            if (value == null) {
                try {
                    value = generateId();
                } catch (final Exception e) {
                    throw new RuntimeException("Unable to generate document ID.", e);
                }
                memoizedId = value;
            }
        }
    }
    return value;
}
```

IMPORTANT: preserve the pre-generated-id constructors (`this(final String id, ...)` at lines ~51-58) which set the id supplier to a constant. Adapt them to set `memoizedId = id` directly instead of `this.id = () -> id`, and remove the `Supplier<String> id` field only after updating every reference (`hashCode()` at ~274 uses `id.get()` → change to `getId()`; `equals` already uses `getId()`). Read the whole file for `id.get()` / `this.id =` references and update each. Keep `generateId()` unchanged.

- [ ] **Step 4: Run the concurrency test + the full document + determinism suites**

Run: `mvn -pl extract-lib test -Dtest='TikaDocumentConcurrencyTest,PstFolderFanoutDeterminismTest,ParallelOcrDeterminismTest' -DfailIfNoTests=false`
Expected: PASS (all). Determinism unchanged (same id values, only safely published).

- [ ] **Step 5: Commit**

```bash
git add extract-lib/src/main/java/org/icij/extract/document/TikaDocument.java \
        extract-lib/src/test/java/org/icij/extract/document/TikaDocumentConcurrencyTest.java
git commit -m "fix(pst): harden shared TikaDocument root (snapshot getEmbeds, safely-published getId)"
```

---

### Task 8: Full-suite gate + fresh multi-agent review

**Files:** none (verification only).

- [ ] **Step 1: Run the FULL extract-lib suite**

Run: `mvn -pl extract-lib test`
Expected: green except the two known pre-existing native-Tesseract failures (`Tess4JOCRParserTest`, `ExtractorTest.test_ocr_confidence`). Record the run count and compare to the pre-remediation baseline (316 run / 15 fail = 8F+7E). New tests (guardian rewrite, fork-context, legacy naming, stdout, cleanup, TikaDocument concurrency) must be green and raise the run count; the failing set must remain exactly the two native-Tesseract classes.

- [ ] **Step 2: Fresh `/code-review max` on the branch (concurrency + byte-identity focus)**

Invoke `/code-review max` scoped to the branch diff vs `master` (d821bc6), instructing finders to focus on concurrency and byte-identity. VERIFY the finder agents actually ran (a prior run returned empty purely due to a usage-limit failure); do NOT trust a "no findings" result unless the finders produced real output. Triage each finding: fix confirmed defects (new task per fix wave), record accepted-with-rationale items.

- [ ] **Step 3: Whole-branch review**

Run `superpowers:requesting-code-review` for a final whole-branch review. Confirm: byte-identity invariant (serial==fan-out) proven by the DigestIdentifier guardian; no shared mutable state across folder threads; nested PSTs serial; handle lifecycle safe on abort; TikaDocument root safe under concurrency. Address any Critical/Important before declaring merge-ready.

- [ ] **Step 4: Update the SDD ledger**

Append the remediation increment (tasks, commits, review verdicts) to `.superpowers/sdd/progress.md` under the PST fan-out section.

---

## Self-Review

**Spec coverage:**
- #1 fork isolation (fork-owned context, self-registered, omits PstFanoutConfig) → Task 1 + Task 2. ✓
- #2 reentrant pool starvation (subsumed; guard) → Task 2 (mechanism) + note on fixture limitation. ✓
- #3 dual-resolution (legacyUntitledNaming, extract-lib side) → Task 3; datashare wiring out of scope (gated Task 7). ✓
- #4 stdout suppression on pool threads → Task 4. ✓
- #5 handle-close-during-read on interrupt → Task 5. ✓
- #6 LOW batch (interrupt status, failed-open cache, lazy folder paths, fork stack test) → Task 6. ✓
- #7a PST_FOLDER_PATH divergence → folded into Task 1 guardian tuple (assert + reconcile). ✓
- #7b getEmbeds snapshot, #7c getId safe publish → Task 7. ✓
- #7d global pool → accept by design (documented in spec; no task). ✓
- Guardian test rewrite (DigestIdentifier, depth>=2, full tuple, repeats, must-fail-then-pass) → Task 1. ✓
- Full-suite gate + fresh /code-review max + whole-branch review → Task 8. ✓

**Placeholder scan:** each code step shows real code grounded in the actual files read. Soft spots called out explicitly: Task 1 Step 1 flags "verify the exact Extractor(DocumentFactory, Options) ctor + digest metadata key against DatashareExtractIntegrationTest/ParallelOcrDeterminismTest" (intrinsic to matching an existing test-construction API); Task 4 Step 1 flags "read PstStdoutFilter for its exact lifecycle API" (the depth-counter method names must be confirmed in the source).

**Type consistency:** `fork()` produces forks whose `parseContextForTest()` (Task 2) is consumed only in tests; `legacyUntitledName(int)` / `isLegacyUntitledNaming()` / `nextUntitledName()` (Task 3) are internally consistent; `awaitQuietly`/`awaitQuietlyForTest` (Task 5) consistent; `pushForTest` (Task 6) consumed in the fork test; `getEmbeds()`/`getId()` (Task 7) signatures unchanged for callers. The new `legacyUntitledNaming` ctor arg is threaded through all four EmbedSpawner constructors consistently (primary sets it; convenience ctors pass false; copy-ctor copies template's).
