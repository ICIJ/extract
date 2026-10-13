# OST/PST intra-parse folder fan-out (B3 increment 1) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Parallelize a single OST/PST parse across its folders so one giant mailbox uses many cores, while embed ids / `X-TIKA:digest:*` / artifact filenames / `OCR_PARSER` stay byte-identical to the serial walk.

**Architecture:** The `.ost` file stays the single logical root. `ResilientOutlookPSTParser` enumerates every folder (flat, via `PstFolderPathResolver.folderPaths`) and submits one folder-emit task per folder to a shared bounded parse pool. Each task opens its own `PSTFile` handle, navigates to its folder by descriptor id, and emits that folder's direct messages through its own forked `EmbedSpawner` (own DFS stack), while a shared thread-safe reconciliation object holds the dedup set + counters. Orphan recovery runs serially after the pool joins. B2's `StreamingSpewCoordinator` is already multi-producer safe and is unchanged.

**Tech Stack:** Java 21, Maven, Tika, java-libpst (`com.pff`), JUnit 4 + fest-assert. Build/test in the extract repo.

## Global Constraints

- Branch off `master` (extract default branch); ff/rebase, no squash; commit via `/commit`; no Claude attribution.
- **Do NOT bump `extract` version on the feature branch** — versioning happens at release.
- Byte-identity invariant is sacred: embed ids / `X-TIKA:digest:*` / artifact filenames / `OCR_PARSER` must be identical between fan-out and serial modes (on-demand download + artifact cache depend on it).
- Full extract-lib suite must stay green except the known pre-existing native-Tesseract failures.
- New options: `pstFolderFanout` (boolean, default `true`, off-switch retained); `pstParseParallelism` (int, default = `Runtime.getRuntime().availableProcessors()`), a SEPARATE pool from `ocrParallelism`.
- Spec: `docs/superpowers/specs/2026-06-29-ost-folder-fanout-design.md`.

---

### Task 1: Add `pstFolderFanout` + `pstParseParallelism` options and a lazy parse pool to `Extractor`

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/extract/extractor/Extractor.java`
- Test: `extract-lib/src/test/java/org/icij/extract/extractor/PstFanoutOptionsTest.java` (create)

**Interfaces:**
- Produces: `boolean Extractor.isPstFolderFanout()`, `int Extractor.getPstParseParallelism()`, package-private `ExecutorService Extractor.pstParseExecutor()` (lazy), `ExecutorService Extractor.pstParseExecutorOrNull()`. `close()` shuts the parse pool in addition to the OCR pool. NOTE: methods are named `pstParseExecutor*` (NOT `parseExecutor*`) to avoid colliding with the pre-existing `parseExecutor` watchdog field; the backing field is `pstParseExecutor`.

- [ ] **Step 1: Write the failing test**

```java
package org.icij.extract.extractor;

import org.icij.task.Options;
import org.junit.Test;

import java.util.Map;

import static org.fest.assertions.Assertions.assertThat;

public class PstFanoutOptionsTest {

    @Test
    public void testDefaults() throws Exception {
        try (Extractor extractor = new Extractor(Options.from(Map.of()))) {
            assertThat(extractor.isPstFolderFanout()).isTrue();
            assertThat(extractor.getPstParseParallelism())
                    .isEqualTo(Runtime.getRuntime().availableProcessors());
            // No parse pool is created until first use.
            assertThat(extractor.parseExecutorOrNull()).isNull();
        }
    }

    @Test
    public void testOverridesAndLazyPoolClosedCleanly() throws Exception {
        try (Extractor extractor = new Extractor(Options.from(Map.of(
                "pstFolderFanout", "false",
                "pstParseParallelism", "4")))) {
            assertThat(extractor.isPstFolderFanout()).isFalse();
            assertThat(extractor.getPstParseParallelism()).isEqualTo(4);
            // Force lazy creation, then confirm close() tears it down without throwing.
            assertThat(extractor.parseExecutor()).isNotNull();
            assertThat(extractor.parseExecutorOrNull()).isNotNull();
        }
        // try-with-resources close() must not throw.
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl extract-lib test -Dtest=PstFanoutOptionsTest -DfailIfNoTests=false`
Expected: FAIL — `isPstFolderFanout()` / `getPstParseParallelism()` / `parseExecutor()` do not exist (compile error).

- [ ] **Step 3: Add the options, fields, lazy pool, getters, and close wiring**

In the `@Option` block (near `Extractor.java:127-141`), add:

```java
@Option(name = "pstFolderFanout", description = "Parse the folders of a single PST/OST mailbox " +
        "in parallel across a shared bounded pool, instead of one thread per file. Embed ids, " +
        "digests and artifact filenames are byte-identical to the serial walk.")
@Option(name = "pstParseParallelism", description = "Number of PST/OST folder-walk tasks run in " +
        "parallel across all in-flight mailboxes. Separate from ocrParallelism.")
```

With the other field defaults (near `Extractor.java:190-197`):

```java
private boolean pstFolderFanout = true;
private int pstParseParallelism = Runtime.getRuntime().availableProcessors();
// Null until first use; created lazily by parseExecutor(), mirroring the OCR pool.
private volatile ExecutorService parseExecutor = null;
```

In the options-parsing block (near `Extractor.java:281-289`), add:

```java
options.get("pstFolderFanout", "true").parse().asBoolean()
        .ifPresent(b -> this.pstFolderFanout = b);
options.get("pstParseParallelism", String.valueOf(Runtime.getRuntime().availableProcessors()))
        .parse().asInteger().ifPresent(n -> this.pstParseParallelism = Math.max(1, n));
```

With the other getters (near `Extractor.java:361-377`):

```java
public boolean isPstFolderFanout() { return pstFolderFanout; }
public int getPstParseParallelism() { return pstParseParallelism; }

ExecutorService parseExecutorOrNull() { return parseExecutor; }

// Lazily create the shared PST folder-walk pool on first fan-out use, mirroring ocrExecutor().
synchronized ExecutorService parseExecutor() {
    if (parseExecutor == null) {
        parseExecutor = Executors.newFixedThreadPool(pstParseParallelism, new ThreadFactory() {
            private final java.util.concurrent.atomic.AtomicInteger n =
                    new java.util.concurrent.atomic.AtomicInteger();
            @Override public Thread newThread(final Runnable r) {
                final Thread t = new Thread(r, "extract-pst-walk-" + n.incrementAndGet());
                t.setDaemon(true);
                return t;
            }
        });
    }
    return parseExecutor;
}
```

In `close()` (near `Extractor.java:409-411`), add the parse-pool teardown alongside the OCR pool:

```java
if (parseExecutor != null) { parseExecutor.shutdownNow(); parseExecutor = null; }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -pl extract-lib test -Dtest=PstFanoutOptionsTest -DfailIfNoTests=false`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add extract-lib/src/main/java/org/icij/extract/extractor/Extractor.java \
        extract-lib/src/test/java/org/icij/extract/extractor/PstFanoutOptionsTest.java
git commit -m "feat(pst): add pstFolderFanout + pstParseParallelism options and lazy parse pool"
```

---

### Task 2: Deterministic per-parent `untitled` naming in `EmbedSpawner`

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/extract/extractor/EmbedSpawner.java` (the two `String.format("untitled_%d", ++untitled)` sites at ~line 210 and ~line 278)
- Test: `extract-lib/src/test/java/org/icij/extract/extractor/EmbedSpawnerUntitledTest.java` (create)

**Interfaces:**
- Produces: package-private static `String EmbedSpawner.untitledName(String parentId, int siblingOrdinal)` — pure, order-independent. Replaces the global `untitled` counter so a nameless non-inline embed gets the same name regardless of walk order (and is reproduced identically by the serial on-demand walk).

**Context:** Today nameless non-inline embeds are named with a per-spawner running counter (`++untitled`). Under fan-out the counter would be assigned in nondeterministic order, changing the name (and thus id / artifact filename) of such embeds. The corpus shows this path is practically never hit for OST (every embed has a per-parent name), but we make it order-independent as cheap insurance.

- [ ] **Step 1: Write the failing test**

```java
package org.icij.extract.extractor;

import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

public class EmbedSpawnerUntitledTest {

    @Test
    public void testUntitledNameIsDeterministicPerParentAndOrdinal() {
        // Same parent + same sibling ordinal -> same name, regardless of global walk order.
        assertThat(EmbedSpawner.untitledName("parentA", 0))
                .isEqualTo(EmbedSpawner.untitledName("parentA", 0));
        // Different ordinal under the same parent -> different name.
        assertThat(EmbedSpawner.untitledName("parentA", 0))
                .isNotEqualTo(EmbedSpawner.untitledName("parentA", 1));
        // Different parent -> different name (avoids collisions across parents).
        assertThat(EmbedSpawner.untitledName("parentA", 0))
                .isNotEqualTo(EmbedSpawner.untitledName("parentB", 0));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl extract-lib test -Dtest=EmbedSpawnerUntitledTest -DfailIfNoTests=false`
Expected: FAIL — `untitledName` is not defined (compile error).

- [ ] **Step 3: Add `untitledName` and use it at both sites; track a per-parent sibling ordinal**

Add the pure helper (anywhere among the private/static helpers in `EmbedSpawner`):

```java
// Deterministic, order-independent name for a non-inline embed that has no resource name.
// Derived from the immediate parent id + the embed's ordinal among its parent's children, so
// the parallel index and the serial on-demand walk produce the same name for the same embed.
static String untitledName(final String parentId, final int siblingOrdinal) {
    return "untitled_" + parentId + "_" + siblingOrdinal;
}
```

Replace the `untitled` field with a per-parent ordinal map (a single-slot "reset when the parent changes" counter is WRONG: DFS that descends into a nameless-bearing child and returns to the parent would reset and re-issue ordinal 0 to a later sibling, colliding). A map keyed by parent id keeps a stable, monotonic ordinal per parent regardless of DFS interleaving:

```java
// Per-parent monotonic ordinal for nameless non-inline embeds, so each gets an
// order-independent name derived from its immediate parent id + its 0-based index among that
// parent's nameless children. A map (not a single reset-on-change slot) so a parent revisited
// after a nested descent keeps counting instead of re-issuing ordinal 0.
private final java.util.Map<String, Integer> untitledOrdinalsByParent = new java.util.HashMap<>();
```

At both naming sites (in `spawnEmbedded` ~line 208-211 and `spawnEmbeddedDeferred` ~line 276-279), replace:

```java
if (null == name || name.isEmpty()) {
    name = String.format("untitled_%d", ++untitled);
}
```

with:

```java
if (null == name || name.isEmpty()) {
    final String parentId = tikaDocumentStack.getLast().getId();
    final int ordinal = untitledOrdinalsByParent.merge(parentId, 1, Integer::sum) - 1;
    name = untitledName(parentId, ordinal);
}
```

Delete the now-unused `private int untitled = 0;` field. (Each EmbedSpawner — including each fork in Task 3 — owns its own map; a parent never spans forks because a message-subtree is parsed atomically on one thread, so a per-fork map is correct.)

- [ ] **Step 4: Run the new test and the existing EmbedSpawner-dependent tests**

Run: `mvn -pl extract-lib test -Dtest=EmbedSpawnerUntitledTest,ParallelOcrDeterminismTest -DfailIfNoTests=false`
Expected: PASS (all). `ParallelOcrDeterminismTest` confirms named embeds are unaffected.

- [ ] **Step 5: Commit**

```bash
git add extract-lib/src/main/java/org/icij/extract/extractor/EmbedSpawner.java \
        extract-lib/src/test/java/org/icij/extract/extractor/EmbedSpawnerUntitledTest.java
git commit -m "feat(pst): make untitled-embed naming deterministic per parent"
```

---

### Task 3: `EmbedSpawner.fork()` sharing config + memory budget, with an independent stack

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/extract/extractor/EmbedSpawner.java`
- Test: `extract-lib/src/test/java/org/icij/extract/extractor/EmbedSpawnerForkTest.java` (create)

**Interfaces:**
- Produces: `EmbedSpawner EmbedSpawner.fork()` — returns a new spawner that SHARES every collaborator (root, context, outputPath, handlerFunction, embedMemoryBudgetBytes, tmp, memoryPressureHigh, ocrExecutorSupplier, ocrEnabled, progress, digester, ocrFanout, ocrMinImageBytes, ocrParserClassName, sink) AND the `reserved` memory-budget `AtomicLong`, but has its own `tikaDocumentStack` (seeded with root) and its own per-parent untitled state. Package-private `AtomicLong EmbedSpawner.reservedBudget()` test accessor.

**Context:** The `reserved` `AtomicLong` (the live embed-memory reservation) is currently `new AtomicLong()` per spawner. For the global `embedMemoryBudgetMb` to hold across all concurrent walkers, every fork must share ONE `reserved`. The `tikaDocumentStack` is the DFS parent stack and must NOT be shared (a shared stack would hand a walker the wrong parent → wrong embed id).

- [ ] **Step 1: Write the failing test**

```java
package org.icij.extract.extractor;

import org.apache.tika.io.TemporaryResources;
import org.apache.tika.parser.ParseContext;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.TikaDocument;
import org.junit.Test;

import java.nio.file.Paths;

import static org.fest.assertions.Assertions.assertThat;

public class EmbedSpawnerForkTest {

    private EmbedSpawner newSerialSpawner() {
        final TikaDocument root = new DocumentFactory().create(Paths.get("/tmp/fake-root.ost"));
        // Serial constructor: no fan-out, no OCR. Sufficient to exercise fork()'s field sharing.
        return new EmbedSpawner(root, new ParseContext(), null,
                w -> new org.apache.tika.sax.BodyContentHandler(w),
                64L * 1024 * 1024, new TemporaryResources(), () -> false);
    }

    @Test
    public void testForkSharesBudgetButNotStack() {
        final EmbedSpawner base = newSerialSpawner();
        final EmbedSpawner forked = base.fork();

        // Same shared budget instance: a reservation on one is visible to the other.
        assertThat(forked.reservedBudget()).isSameAs(base.reservedBudget());

        // Independent DFS stacks: pushing on the fork must not change the base's stack depth.
        assertThat(forked.stackDepth()).isEqualTo(1); // seeded with root only
        assertThat(base.stackDepth()).isEqualTo(1);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl extract-lib test -Dtest=EmbedSpawnerForkTest -DfailIfNoTests=false`
Expected: FAIL — `fork()`, `reservedBudget()`, `stackDepth()` not defined (compile error).

- [ ] **Step 3: Make `reserved` shareable, add a copy-constructor + `fork()` + test accessors**

Change the `reserved` field so it can be shared (remove `final` initializer, set in constructors):

```java
private final AtomicLong reserved;
```

In the existing primary constructor (the 14-arg one at ~line 111), initialize a fresh budget:

```java
this.reserved = new AtomicLong();
```

Add a private copy-constructor that shares all fields + the budget but starts a fresh stack, then a public `fork()`:

```java
// Shares every collaborator AND the live memory budget with `template`, but gets its own DFS
// stack (seeded with the shared root) and its own per-parent untitled state. Used by the PST
// folder fan-out so each walker has an isolated parent stack while the global embed-memory
// budget stays shared across all walkers.
private EmbedSpawner(final EmbedSpawner template) {
    super(template.root, template.context);
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
    tikaDocumentStack.add(template.root); // fresh stack, seeded with the shared root
}

public EmbedSpawner fork() {
    return new EmbedSpawner(this);
}

// Test accessors (package-private).
AtomicLong reservedBudget() { return reserved; }
int stackDepth() { return tikaDocumentStack.size(); }
```

Note: `root`, `context`, and `tikaDocumentStack` come from / are declared in this class hierarchy already (`super(root, context)` is how the existing constructors set root/context; `tikaDocumentStack` is a `final` field initialized at declaration). If `root`/`context` are private in `EmbedParser`, expose them via existing protected getters or store `template.root`/`template.context` through the same path the existing constructors use; the existing constructors already call `super(root, context)`, so pass `template.root`/`template.context` the same way (add protected accessors in `EmbedParser` if not present: `protected TikaDocument root()` / `protected ParseContext context()`).

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -pl extract-lib test -Dtest=EmbedSpawnerForkTest,ParallelOcrDeterminismTest -DfailIfNoTests=false`
Expected: PASS (all).

- [ ] **Step 5: Commit**

```bash
git add extract-lib/src/main/java/org/icij/extract/extractor/EmbedSpawner.java \
        extract-lib/src/main/java/org/icij/extract/extractor/EmbedParser.java \
        extract-lib/src/test/java/org/icij/extract/extractor/EmbedSpawnerForkTest.java
git commit -m "feat(pst): add EmbedSpawner.fork() sharing budget with an isolated parse stack"
```

---

### Task 4: Make `ResilientOutlookPSTParser` reconciliation state thread-safe (serial behavior unchanged)

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/extract/parser/ResilientOutlookPSTParser.java` (the `EmissionContext` inner class, ~line 609-693, and its construction in `parse`, ~line 110)
- Test: existing PST tests (serial path) — no new test; this is a structural refactor guarded by the suite.

**Interfaces:**
- Produces: a new package-private static `Reconciliation` holding the dedup set + counters + the attachment-integrity flag, all thread-safe. `EmissionContext` keeps `xhtml` + `extractor` and delegates dedup/counter calls to a shared `Reconciliation`. New constructor `EmissionContext(XHTMLContentHandler, EmbeddedDocumentExtractor, Reconciliation)`.

- [ ] **Step 1: Extract a thread-safe `Reconciliation` and delegate from `EmissionContext`**

Add the new inner class (next to `EmissionContext`):

```java
// Shared, thread-safe across all folder-walk tasks of one parse: the dedup set, the emitted
// counter, and the OST-2013 attachment-integrity counters.
private static final class Reconciliation {
    private final Set<Long> emittedDescriptorIds = ConcurrentHashMap.newKeySet();
    private final AtomicInteger emittedCount = new AtomicInteger();
    private volatile boolean checkAttachmentIntegrity = false;
    private final AtomicInteger unreadableAttachments = new AtomicInteger();
    private final AtomicInteger recoveredAttachments = new AtomicInteger();
    private final AtomicInteger unrecoveredAttachments = new AtomicInteger();
    private final AtomicInteger encryptedAttachments = new AtomicInteger();

    boolean markEmitted(final long id) { return emittedDescriptorIds.add(id); }
    void unmarkEmitted(final long id) { emittedDescriptorIds.remove(id); }
    boolean alreadyEmitted(final long id) { return emittedDescriptorIds.contains(id); }
    void incrementEmitted() { emittedCount.incrementAndGet(); }
    int emittedCount() { return emittedCount.get(); }
    void enableAttachmentIntegrityCheck() { checkAttachmentIntegrity = true; }
    boolean shouldCheckAttachmentIntegrity() { return checkAttachmentIntegrity; }
    void incrementUnreadableAttachments() { unreadableAttachments.incrementAndGet(); }
    int unreadableAttachments() { return unreadableAttachments.get(); }
    void incrementRecoveredAttachments() { recoveredAttachments.incrementAndGet(); }
    int recoveredAttachments() { return recoveredAttachments.get(); }
    void incrementUnrecoveredAttachments() { unrecoveredAttachments.incrementAndGet(); }
    int unrecoveredAttachments() { return unrecoveredAttachments.get(); }
    void incrementEncryptedAttachments() { encryptedAttachments.incrementAndGet(); }
    int encryptedAttachments() { return encryptedAttachments.get(); }
}
```

Replace `EmissionContext`'s own state fields with a `Reconciliation` reference and delegate every existing method (`markEmitted`, `unmarkEmitted`, `alreadyEmitted`, `incrementEmitted`, `emittedCount`, `enableAttachmentIntegrityCheck`, `shouldCheckAttachmentIntegrity`, and the four attachment-integrity inc/get pairs) to it. Keep the `xhtml`, `extractor`, and `parseEmbedded(...)` members as they are:

```java
private static final class EmissionContext {
    private final XHTMLContentHandler xhtml;
    private final EmbeddedDocumentExtractor extractor;
    private final Reconciliation reconciliation;

    private EmissionContext(final XHTMLContentHandler xhtml, final EmbeddedDocumentExtractor extractor,
                            final Reconciliation reconciliation) {
        this.xhtml = xhtml;
        this.extractor = extractor;
        this.reconciliation = reconciliation;
    }

    private void enableAttachmentIntegrityCheck() { reconciliation.enableAttachmentIntegrityCheck(); }
    private boolean shouldCheckAttachmentIntegrity() { return reconciliation.shouldCheckAttachmentIntegrity(); }
    private void incrementUnreadableAttachments() { reconciliation.incrementUnreadableAttachments(); }
    private int unreadableAttachments() { return reconciliation.unreadableAttachments(); }
    private void incrementRecoveredAttachments() { reconciliation.incrementRecoveredAttachments(); }
    private int recoveredAttachments() { return reconciliation.recoveredAttachments(); }
    private void incrementUnrecoveredAttachments() { reconciliation.incrementUnrecoveredAttachments(); }
    private int unrecoveredAttachments() { return reconciliation.unrecoveredAttachments(); }
    private void incrementEncryptedAttachments() { reconciliation.incrementEncryptedAttachments(); }
    private int encryptedAttachments() { return reconciliation.encryptedAttachments(); }
    private boolean markEmitted(final long id) { return reconciliation.markEmitted(id); }
    private void unmarkEmitted(final long id) { reconciliation.unmarkEmitted(id); }
    private boolean alreadyEmitted(final long id) { return reconciliation.alreadyEmitted(id); }
    private void incrementEmitted() { reconciliation.incrementEmitted(); }
    private int emittedCount() { return reconciliation.emittedCount(); }

    private void parseEmbedded(final TikaInputStream messageStream, final Metadata metadata)
            throws SAXException, IOException, TikaException {
        extractor.parseEmbedded(messageStream, xhtml, metadata, true);
    }
}
```

Add imports: `java.util.concurrent.ConcurrentHashMap`, `java.util.concurrent.atomic.AtomicInteger`.

Update the single construction site in `parse()` (~line 110-111) to create a `Reconciliation` and pass it:

```java
final Reconciliation reconciliation = new Reconciliation();
final EmissionContext emission = new EmissionContext(xhtml, extractor, reconciliation);
```

(If `enableAttachmentIntegrityCheck()` is currently called on `emission` in `extractAllMessages`, it still works via the delegate — no change there.)

- [ ] **Step 2: Run the existing PST parser tests (serial path must be unchanged)**

Run: `mvn -pl extract-lib test -Dtest='ResilientOutlookPSTParser*Test,StreamingSpewPstStartupTest,*Pst*Test' -DfailIfNoTests=false`
Expected: PASS (serial behavior identical).

- [ ] **Step 3: Commit**

```bash
git add extract-lib/src/main/java/org/icij/extract/parser/ResilientOutlookPSTParser.java
git commit -m "refactor(pst): extract thread-safe Reconciliation from EmissionContext"
```

---

### Task 5: Parallel folder walk + Extractor wiring (fan-out ON path)

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/extract/parser/ResilientOutlookPSTParser.java` (`extractAllMessages`, ~line 138-162)
- Modify: `extract-lib/src/main/java/org/icij/extract/extractor/Extractor.java` (`getTikaDocument` SPAWN branch, ~line 900-905)
- Create: `extract-lib/src/main/java/org/icij/extract/parser/PstFanoutConfig.java`
- Test: `extract-lib/src/test/java/org/icij/extract/extractor/PstFolderFanoutDeterminismTest.java` (create)

**Interfaces:**
- Consumes: `EmbedSpawner.fork()` (Task 3), thread-safe `Reconciliation`/`EmissionContext` (Task 4), `Extractor.parseExecutor()` + `isPstFolderFanout()` (Task 1).
- Produces: `PstFanoutConfig` (record carrying `boolean enabled` + `Supplier<ExecutorService> executor`), set into the `ParseContext` by `Extractor`. `ResilientOutlookPSTParser` reads it; when enabled and the extractor is an `EmbedSpawner`, it walks folders in parallel.

- [ ] **Step 1: Write the failing determinism test (the guardian)**

```java
package org.icij.extract.extractor;

import org.icij.extract.document.EmbeddedTikaDocument;
import org.icij.extract.document.TikaDocument;
import org.icij.spewer.Spewer;
import org.icij.task.Options;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static org.fest.assertions.Assertions.assertThat;

/**
 * Sacred-invariant guard: a PST parsed with folder fan-out ON must produce the SAME SET of
 * {embed id, X-TIKA:digest:*, artifact filename(=id), name} as the serial walk. Fan-out changes
 * only the ORDER independent message-subtrees are processed in; no identity component crosses a
 * message-subtree boundary. OCR is disabled so the test does not depend on Tesseract.
 */
public class PstFolderFanoutDeterminismTest {

    private static final String PST = "/documents/pst/testPST.pst";

    private TreeSet<String> identitySet(boolean fanout) throws Exception {
        try (Extractor extractor = new Extractor(Options.from(Map.of(
                "pstFolderFanout", String.valueOf(fanout),
                "pstParseParallelism", "8",
                "ocr", "false",
                "progressHeartbeatInterval", "0")))) {
            TikaDocument doc = extractor.extract(
                    Paths.get(getClass().getResource(PST).toURI()));
            // Drive the parse to completion (PST emits all content as embeds).
            try (java.io.Reader r = doc.getReader()) { Spewer.toString(r); }
            TreeSet<String> ids = new TreeSet<>();
            collect(doc, ids);
            return ids;
        }
    }

    private void collect(TikaDocument doc, TreeSet<String> ids) throws Exception {
        for (EmbeddedTikaDocument embed : doc.getEmbeds()) {
            ids.add(embed.getId() + "|"
                    + embed.getMetadata().get("X-TIKA:digest:SHA256") + "|"
                    + embed.getMetadata().get(org.apache.tika.metadata.TikaCoreProperties.RESOURCE_NAME_KEY));
            collect(embed, ids);
        }
    }

    @Test(timeout = 120_000)
    public void testFanoutProducesIdenticalIdentitySetAsSerial() throws Exception {
        TreeSet<String> serial = identitySet(false);
        TreeSet<String> fanout = identitySet(true);
        assertThat(serial).isNotEmpty();
        assertThat(fanout).isEqualTo(serial);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl extract-lib test -Dtest=PstFolderFanoutDeterminismTest -DfailIfNoTests=false`
Expected: FAIL — fan-out is not wired, so `PstFanoutConfig` is absent / fan-out is a no-op and (until wired) the parser ignores the flag; the test compiles only after Step 3 creates `PstFanoutConfig`. (If it instead passes trivially because both runs are serial, that is acceptable only after Step 3 makes fan-out real; ensure Step 3 lands before trusting a pass.)

- [ ] **Step 3: Create `PstFanoutConfig`, wire it in `Extractor`, and add the parallel walk**

Create `extract-lib/src/main/java/org/icij/extract/parser/PstFanoutConfig.java`:

```java
package org.icij.extract.parser;

import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

/** Carries PST/OST folder fan-out settings through the Tika ParseContext. */
public record PstFanoutConfig(boolean enabled, Supplier<ExecutorService> executor) {}
```

In `Extractor.getTikaDocument`, in the `EmbedHandling.SPAWN` branch right after the `EmbedSpawner` is set into the context (~line 905), also set the fan-out config:

```java
context.set(org.icij.extract.parser.PstFanoutConfig.class,
        new org.icij.extract.parser.PstFanoutConfig(pstFolderFanout, this::pstParseExecutor));
```

In `ResilientOutlookPSTParser.extractAllMessages`, replace the single `walkFolder(pstFile.getRootFolder(), "/", emission)` call with a fan-out branch. The folder enumeration reuses `PstFolderPathResolver.folderPaths(pstFile)` (already used for orphan recovery), and each task loads its folder via `PSTObject.detectAndLoadPSTObject`. Add the `reconciliation` and base `EmbedSpawner` as parameters threaded from `parse()` (pass `reconciliation` and the base extractor down, or read the `PstFanoutConfig` and base spawner from a field set in `parse`).

```java
private void extractAllMessages(final PSTFile pstFile, final String pstPath,
                                final EmissionContext emission, final Metadata metadata,
                                final Reconciliation reconciliation,
                                final PstFanoutConfig fanout) throws Exception {
    if (pstFile.getPSTFileType() == PSTFile.PST_TYPE_2013_UNICODE) {
        emission.enableAttachmentIntegrityCheck();
    }
    final List<Integer> messageDescriptorIds = enumerateMessageDescriptorIds(pstFile, pstPath);
    final Map<Integer, String> folderPaths = resolveFolderPaths(pstFile, pstPath);

    final boolean canFanOut = fanout != null && fanout.enabled()
            && emission.extractor instanceof EmbedSpawner
            && folderPaths != null && folderPaths.size() > 1;

    if (canFanOut) {
        walkFoldersParallel(pstPath, folderPaths, emission, reconciliation, fanout.executor().get());
    } else {
        walkFolder(pstFile.getRootFolder(), "/", emission);
    }

    if (messageDescriptorIds != null) {
        final Map<Integer, String> recoveryPaths = (folderPaths != null) ? folderPaths
                : resolveFolderPaths(pstFile, pstPath);
        recoverOrphans(messageDescriptorIds, pstFile, recoveryPaths, emission);
    }
    // ... existing reconciliation recording (recordReconciliation / recordAttachmentIntegrity) ...
}

// One task per folder. Each task opens its OWN PSTFile handle (libpst is not thread-safe),
// loads its folder by descriptor id, and emits that folder's DIRECT messages through a FORKED
// EmbedSpawner (its own DFS stack). The shared Reconciliation dedups + counts across tasks.
// All PSTFile handles are closed after the pool joins.
private void walkFoldersParallel(final String pstPath, final Map<Integer, String> folderPaths,
                                 final EmissionContext baseEmission, final Reconciliation reconciliation,
                                 final ExecutorService executor) throws Exception {
    final EmbedSpawner baseSpawner = (EmbedSpawner) baseEmission.extractor;
    final Map<Thread, PSTFile> handles = new ConcurrentHashMap<>();
    final List<Future<?>> futures = new ArrayList<>(folderPaths.size());
    try {
        for (final Map.Entry<Integer, String> entry : folderPaths.entrySet()) {
            final int descriptorId = entry.getKey();
            final String folderPath = entry.getValue();
            futures.add(executor.submit(() -> {
                try {
                    final PSTFile own = handles.computeIfAbsent(Thread.currentThread(), t -> openQuietly(pstPath));
                    if (own == null) {
                        return;
                    }
                    final PSTObject object = PSTObject.detectAndLoadPSTObject(own, (long) descriptorId);
                    if (object instanceof PSTFolder) {
                        final EmissionContext walkerEmission =
                                new EmissionContext(baseEmission.xhtml, baseSpawner.fork(), reconciliation);
                        emitFolderMessages((PSTFolder) object, folderPath, walkerEmission);
                    }
                } catch (final Exception | LinkageError e) {
                    logger.warn("PST folder \"{}\" (descriptor {}) parallel walk failed; skipping.",
                            folderPath, descriptorId, e);
                }
            }));
        }
        for (final Future<?> f : futures) {
            try {
                f.get();
            } catch (final ExecutionException e) {
                logger.warn("PST folder task failed.", e.getCause());
            }
        }
    } finally {
        for (final PSTFile h : handles.values()) {
            closeQuietly(h);
        }
    }
}

private static PSTFile openQuietly(final String pstPath) {
    try {
        return new PSTFile(pstPath);
    } catch (final Exception | LinkageError e) {
        LoggerFactory.getLogger(ResilientOutlookPSTParser.class)
                .warn("Could not open a per-walker PSTFile handle for \"{}\".", pstPath, e);
        return null;
    }
}
```

Thread the new params from `parse()`: in `parse()` read `final PstFanoutConfig fanout = context.get(PstFanoutConfig.class);` and pass `reconciliation` + `fanout` into the `extractAllMessages(...)` call. Add imports: `java.util.concurrent.{ExecutorService,Future,ExecutionException}`, `java.util.concurrent.ConcurrentHashMap`, `java.util.ArrayList`, `java.util.List`, `java.util.Map`, `com.pff.PSTObject`, `com.pff.PSTFolder`, `com.pff.PSTFile`.

Note on `emission.extractor` access: `extractor` is private in `EmissionContext`; add a package-private getter `EmbeddedDocumentExtractor extractor() { return extractor; }` and `XHTMLContentHandler xhtml() { return xhtml; }` (or read the fields directly since `walkFoldersParallel` is in the same outer class and `EmissionContext` is a nested class — nested-class private members are accessible from the enclosing class).

- [ ] **Step 4: Run the determinism test + the full PST suite**

Run: `mvn -pl extract-lib test -Dtest='PstFolderFanoutDeterminismTest,ParallelOcrDeterminismTest,StreamingSpewPstStartupTest,*Pst*Test' -DfailIfNoTests=false`
Expected: PASS (all). The determinism test proves the identity set is byte-identical between fan-out and serial.

- [ ] **Step 5: Commit**

```bash
git add extract-lib/src/main/java/org/icij/extract/parser/PstFanoutConfig.java \
        extract-lib/src/main/java/org/icij/extract/parser/ResilientOutlookPSTParser.java \
        extract-lib/src/main/java/org/icij/extract/extractor/Extractor.java \
        extract-lib/src/test/java/org/icij/extract/extractor/PstFolderFanoutDeterminismTest.java
git commit -m "feat(pst): walk PST/OST folders in parallel across a shared parse pool"
```

---

### Task 6: Fan-out startup/deadlock guard

**Files:**
- Test: `extract-lib/src/test/java/org/icij/extract/extractor/PstFolderFanoutStartupTest.java` (create)

**Interfaces:**
- Consumes: the fan-out path (Task 5), `StreamingSpewCoordinator` backpressure.

**Context:** Mirror `StreamingSpewPstStartupTest` but with fan-out ON, a tiny `spewQueueCapacity`, and a small parse pool, to prove the multi-producer spew path streams under backpressure without deadlock.

- [ ] **Step 1: Write the test**

```java
package org.icij.extract.extractor;

import org.icij.extract.document.TikaDocument;
import org.icij.spewer.FieldNames;
import org.icij.spewer.Spewer;
import org.icij.task.Options;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.fest.assertions.Assertions.assertThat;

public class PstFolderFanoutStartupTest {

    private static final String PST = "/documents/pst/testPST.pst";

    private static class CountingSpewer extends Spewer {
        final AtomicInteger docs = new AtomicInteger();
        CountingSpewer() { super(new FieldNames()); }
        @Override
        protected void writeDocument(TikaDocument doc, TikaDocument parent, TikaDocument root, int level) throws IOException {
            try { Spewer.toString(doc.getReader()); } catch (final Exception ignored) { }
            docs.incrementAndGet();
        }
    }

    @Test(timeout = 120_000)
    public void testFanoutStreamsWithTinyQueueWithoutDeadlock() throws Exception {
        final CountingSpewer spewer = new CountingSpewer();
        try (Extractor extractor = new Extractor(Options.from(Map.of(
                "pstFolderFanout", "true",
                "pstParseParallelism", "4",
                "streamingSpew", "true",
                "spewQueueCapacity", "2",   // far smaller than the PST's embed count
                "ocr", "false",
                "progressHeartbeatInterval", "0")))) {
            extractor.extract(Paths.get(getClass().getResource(PST).toURI()), spewer);
        }
        assertThat(spewer.docs.get()).isGreaterThan(2);
    }
}
```

- [ ] **Step 2: Run test to verify it passes (no hang)**

Run: `mvn -pl extract-lib test -Dtest=PstFolderFanoutStartupTest -DfailIfNoTests=false`
Expected: PASS within the timeout (multi-producer streaming under backpressure, no deadlock).

- [ ] **Step 3: Run the FULL extract-lib suite**

Run: `mvn -pl extract-lib test`
Expected: green except the known pre-existing native-Tesseract failures. Note the count to compare against B2's "307 run / only native-Tesseract failures" baseline.

- [ ] **Step 4: Commit**

```bash
git add extract-lib/src/test/java/org/icij/extract/extractor/PstFolderFanoutStartupTest.java
git commit -m "test(pst): guard folder fan-out against the streaming-spew startup deadlock"
```

---

### Task 7: Datashare adoption (gated on the extract release)

**Files:**
- Modify: `pom.xml` (the `extract.version` property) in `../datashare`
- Modify: datashare launch/config where `ocrParallelism` is surfaced, to also pass `pstFolderFanout` / `pstParseParallelism` and the raised memory options.

**Interfaces:**
- Consumes: the released `extract` artifact carrying Tasks 1-6.

**Context:** This task CANNOT complete until the extract changes are released (extract `docs/superpowers` constraint forbids bumping the version on the feature branch). It stacks on B2's pending `feat/streaming-spew-adoption`. Mirrors the B2/A3 adoption pattern.

- [ ] **Step 1: Bump `extract.version` to the new release** (the same release that publishes B2's 9.12.0). Edit `../datashare/pom.xml`.

- [ ] **Step 2: Surface the new options.** Where datashare passes `ocrParallelism` into the extractor options, also pass `pstFolderFanout` (default true) and `pstParseParallelism` (default = CPU). Raise `embedMemoryBudgetMb` and `embedMemoryPressureThreshold` for the higher concurrency (the roadmap memory-headroom item).

- [ ] **Step 3: Build + run the targeted datashare tests.**

Run: `cd ../datashare && mvn -pl datashare-app -am test -Dtest='*Index*Test,*Spewer*Test' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS (ES at `elasticsearch:9200`, Redis at `redis:6379`).

- [ ] **Step 4: Commit** via `/commit` on a branch off the datashare default branch.

- [ ] **Step 5: Box validation on the OST box** (the real OSTs):
  1. Baseline one OST with `pstFolderFanout=false`; record wall-clock + a sample of embed ids (query ES `id` for a known folder's messages).
  2. Re-run the same OST with fan-out ON; assert load climbs from ~3 toward `pstParseParallelism`, the `~1000 docs/2min` rate rises, and the sampled embed ids are UNCHANGED.
  3. `kill -3 <pid>` thread dump shows `extract-pst-walk-*` threads active (not all parked); watch RSS vs the 61 GB box for OOM.

---

## Self-Review

**Spec coverage:**
- Distribution / shared bounded pool → Tasks 1, 5. ✓
- Per-walker `EmbedSpawner` (own stack, shared budget) → Task 3. ✓
- Per-walker `PSTFile` handle via `detectAndLoadPSTObject` → Task 5. ✓
- Thread-safe `EmissionContext` → Task 4. ✓
- Per-parent `untitled` mitigation → Task 2. ✓
- Spew coordinator unchanged (verified multi-producer safe) → no task needed; guarded by Task 6. ✓
- Serial tail (orphan recovery / reconciliation after join) → Task 5 (`extractAllMessages` keeps the serial recovery + recording after the join). ✓
- Determinism guardian test → Task 5. ✓
- Startup/deadlock guard → Task 6. ✓
- Options `pstFolderFanout` (default ON) + separate `pstParseParallelism` → Task 1. ✓
- Datashare adoption + memory tuning + box validation → Task 7. ✓
- Known limit (dominant flat folder) → out of scope by design; not a task. ✓

**Placeholder scan:** No "TBD"/"add error handling"/"similar to Task N" — each code step shows real code. The one soft spot (Task 5 Step 2 "expected to fail") is annotated because the test only compiles after Step 3 creates `PstFanoutConfig`; that is intrinsic to wiring a new context type and is called out explicitly.

**Type consistency:** `fork()` (Task 3) is consumed in Task 5; `Reconciliation` + `EmissionContext(xhtml, extractor, reconciliation)` (Task 4) are consumed in Task 5; `isPstFolderFanout()`/`parseExecutor()` (Task 1) are consumed in Task 5. `PstFanoutConfig(boolean enabled, Supplier<ExecutorService> executor)` defined and used consistently. `untitledName(String, int)` (Task 2) is internal to `EmbedSpawner`.
