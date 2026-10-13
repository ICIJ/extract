# root-stub-first Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a streaming-spew container root visible in the index as a PARTIAL stub as soon as the parse begins emitting children, then enrich it to COMPLETE at parse end.

**Architecture:** On the spew worker thread, write the root stub at the first drained child (the foreground is blocked reading root content for the whole parse, so the worker is the only thread that sees "parse started"). The end-of-parse real root write (same id+path) overwrites the stub with full content; `finalizeRoot` marks it COMPLETE. On abort, refresh the stub's child count while keeping it PARTIAL. Extract-lib owns the coordinator logic; datashare owns the Elasticsearch write behavior.

**Tech Stack:** Java 17, Maven, JUnit 4 + FEST assertions (extract-lib), live Elasticsearch 8.x integration tests (datashare-index).

## Global Constraints

- Extract-lib work is on branch `feat/root-stub-first` in a worktree, based off `master` (`d3e5fb9`). Independent of PR #66 (merged); touches only `Spewer`, `StreamingSpewCoordinator`, and their tests.
- Match existing comment style (`//` with `--`, `->`); do NOT use the em dash character.
- One recoveryStatus vocabulary: `PARTIAL` = incomplete root (any cause); a finished parse flips it to COMPLETE / PST rollup. Do NOT add new status values. Do NOT add a resume-cursor field.
- `nbChildrenEmitted` is the count of durably-indexed children; maintained by stub, finalize, and abort-refresh.
- Datashare tasks are GATED on an extract release containing Tasks 1 and 2. They live in the separate repo the datashare repo on a branch `feat/root-stub-first`, NOT in the extract worktree.
- Commit after each task. Do not push or release (the maintainer releases extract and merges).

---

## Task 1: Spewer API surface (writeRootStub returns boolean, add finalizeAbortedRoot)

Pure API/refactor: no behavior change, all existing coordinator tests stay green. This unblocks Task 2 and the datashare override.

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/spewer/Spewer.java:72` (writeRootStub signature) and after line 90 (new finalizeAbortedRoot)
- Test: `extract-lib/src/test/java/org/icij/spewer/StreamingSpewCoordinatorTest.java:21-55` (RecordingSpewer fixture)

**Interfaces:**
- Produces: `protected boolean writeRootStub(TikaDocument root, long writtenChildren) throws IOException` (default returns `false`); `protected void finalizeAbortedRoot(TikaDocument root, long writtenChildren) throws IOException` (default no-op).

- [ ] **Step 1: Change `writeRootStub` to return boolean in Spewer.java**

Replace the method at line 72-74:

```java
    protected boolean writeRootStub(final TikaDocument root, final long writtenChildren) throws IOException {
        return false; // no-op by default: nothing was written
    }
```

Update its javadoc `@return` by appending this line before the closing `*/` of the writeRootStub javadoc (after the existing `@param writtenChildren` block, around line 71):

```java
     * @return {@code true} if a stub row was actually written, {@code false} if the call was a no-op
     *         (e.g. the root already exists so writing a contentless stub would clobber it)
```

- [ ] **Step 2: Add `finalizeAbortedRoot` default no-op in Spewer.java**

Insert immediately after the `finalizeRoot` method (after line 90):

```java

    /**
     * Refresh the child count on a container ROOT still represented by its early PARTIAL stub because
     * the parse aborted after the stub was written (see {@link #writeRootStub}) but before the real
     * root write. Unlike {@link #finalizeRoot}, this must NOT mark the root complete: the parse did not
     * finish, so the root stays PARTIAL. Called at most once per aborted root, after the worker drained.
     *
     * <p>Default no-op.
     *
     * @param root            the container root still represented by its early PARTIAL stub
     * @param writtenChildren the number of embedded children written before the abort
     */
    protected void finalizeAbortedRoot(final TikaDocument root, final long writtenChildren) throws IOException {
        // no-op by default
    }
```

- [ ] **Step 3: Update the RecordingSpewer fixture to the new signatures**

In `StreamingSpewCoordinatorTest.java`, replace the `writeRootStub` override (lines 41-45) and extend the fixture:

```java
        volatile boolean rootStubExists = false; // simulate a root already indexed (reindex): stub write is a no-op

        @Override
        protected boolean writeRootStub(TikaDocument root, long writtenChildren) {
            rootStubs.add(root.getId());
            rootStubChildCounts.add(writtenChildren);
            return !rootStubExists;
        }

        final List<String> finalizedAbortedRoots = Collections.synchronizedList(new ArrayList<>());
        final List<Long> finalizeAbortedChildCounts = Collections.synchronizedList(new ArrayList<>());

        @Override
        protected void finalizeAbortedRoot(TikaDocument root, long writtenChildren) {
            finalizedAbortedRoots.add(root.getId());
            finalizeAbortedChildCounts.add(writtenChildren);
        }
```

(Leave the existing `finalizeRoot` override and the `finalizedRoots`/`finalizeChildCounts` lists as they are.)

- [ ] **Step 4: Run the existing coordinator tests, expect all green**

Run: `mvn -q -pl extract-lib test -Dtest=StreamingSpewCoordinatorTest`
Expected: PASS (no behavior change; the coordinator statement call `spewer.writeRootStub(...)` compiles unchanged and existing tests are unaffected).

- [ ] **Step 5: Commit**

```bash
git add extract-lib/src/main/java/org/icij/spewer/Spewer.java extract-lib/src/test/java/org/icij/spewer/StreamingSpewCoordinatorTest.java
git commit -m "refactor(spew): return whether writeRootStub wrote, add finalizeAbortedRoot hook"
```

---

## Task 2: root-stub-first in StreamingSpewCoordinator

Write the stub at the first child; rewrite `close()` so abort refreshes the count instead of re-stubbing. Update the three existing tests whose premise changes and add three new ones.

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/spewer/StreamingSpewCoordinator.java` (field ~44, run() ~137-140, close()/writeRootStubIfOrphaned 204-230)
- Test: `extract-lib/src/test/java/org/icij/spewer/StreamingSpewCoordinatorTest.java`

**Interfaces:**
- Consumes: `Spewer.writeRootStub(...) -> boolean`, `Spewer.finalizeAbortedRoot(...)` (Task 1).
- Produces: no new public API. `writtenCount()` (existing) is reused by tests to detect the first drained child.

- [ ] **Step 1: Add the failing test for the early stub on normal completion**

Add to `StreamingSpewCoordinatorTest.java`:

```java
    @Test
    public void testWritesEarlyRootStubOnNormalCompletionThenFinalizes() throws Exception {
        RecordingSpewer spewer = new RecordingSpewer();
        TikaDocument root = doc("root", new StringReader("root-body"));
        TikaDocument e1 = doc("e1", new StringReader("a"));

        try (StreamingSpewCoordinator coord = new StreamingSpewCoordinator(spewer, 16)) {
            coord.start();
            coord.promise();
            coord.ready(new SpewItem(e1, root, root, 1));
            // Wait until the worker has drained the first child; the early stub is written just before
            // that child, so writtenCount() >= 1 proves the stub was already written -- and the real
            // root has NOT been written yet (spew() not yet called), so the stub was not suppressed.
            for (int i = 0; i < 200 && coord.writtenCount() < 1; i++) {
                Thread.sleep(5);
            }
            assertThat(coord.writtenCount()).isEqualTo(1L);
            assertThat(spewer.rootStubs).containsOnly("root"); // visible up front, before the real root

            coord.spew(root); // foreground now writes the real root, driving completion
        }

        // Real root written + at least one child -> finalized COMPLETE with the final count; the early
        // stub is NOT re-written and the abort refresh does not run.
        assertThat(spewer.written).contains("root", "e1");
        assertThat(spewer.finalizedRoots).containsOnly("root");
        assertThat(spewer.finalizeChildCounts).containsOnly(1L);
        assertThat(spewer.finalizedAbortedRoots).isEmpty();
    }
```

- [ ] **Step 2: Run it, expect failure**

Run: `mvn -q -pl extract-lib test -Dtest=StreamingSpewCoordinatorTest#testWritesEarlyRootStubOnNormalCompletionThenFinalizes`
Expected: FAIL (`rootStubs` is empty; the coordinator does not yet write an early stub).

- [ ] **Step 3: Add the `rootStubWritten` field and the early-stub write in run()**

In `StreamingSpewCoordinator.java`, add the field after line 44 (`private volatile boolean rootWritten;`):

```java
    private volatile boolean rootStubWritten;
```

In `run()`, replace the first-child capture (lines 138-140):

```java
                if (seenRoot == null) {
                    seenRoot = item.root();
                }
```

with:

```java
                if (seenRoot == null) {
                    seenRoot = item.root();
                    writeEarlyRootStub();
                }
```

Add this method (place it next to `writeRootStubIfOrphaned`, which Step 5 removes):

```java
    /**
     * Write the container root as a contentless PARTIAL stub as soon as the parse begins emitting
     * children, so the root is visible in the index during a long parse instead of only at the end
     * (the foreground is blocked reading the root's content the whole time). Best-effort and once per
     * parse, on the worker thread. The end-of-parse real root write (same id+path) later overwrites the
     * stub with full content, and finalizeRoot marks it complete. A duplicate root needs no stub (its
     * children are skipped). The !rootWritten guard avoids clobbering an already-written real root in
     * the degenerate case where the root body was read before the first child was drained.
     */
    private void writeEarlyRootStub() {
        final TikaDocument root = seenRoot;
        if (rootStubWritten || rootWritten || root == null || root.isDuplicate()) {
            return;
        }
        try {
            rootStubWritten = spewer.writeRootStub(root, writtenEmbeds.get());
        } catch (final Throwable t) {
            logger.error("failed to write early root stub for {}", root.getId(), t);
        }
    }
```

- [ ] **Step 4: Run the new test, expect it to pass**

Run: `mvn -q -pl extract-lib test -Dtest=StreamingSpewCoordinatorTest#testWritesEarlyRootStubOnNormalCompletionThenFinalizes`
Expected: PASS.

- [ ] **Step 5: Rewrite close() to refresh the count on abort**

In `StreamingSpewCoordinator.java`, replace `close()` and `writeRootStubIfOrphaned()` (lines 204-230) with:

```java
    @Override
    public void close() {
        shutdownWorker();
        finalizeOrStubAbortedRoot();
    }

    /**
     * On close after an aborted parse (the foreground never completed the real root write), keep the
     * streamed children from being orphaned and record their count. If this run's early stub was
     * written, refresh its child count while keeping it PARTIAL; otherwise write the stub now (fallback
     * when the early stub failed, or when writeRootStub reported the root already existed so we must not
     * refresh a pre-existing complete root's count). Best-effort; runs after the worker drained, so
     * writtenEmbeds/seenRoot are final. A duplicate root or a root with no written children needs
     * nothing.
     */
    private void finalizeOrStubAbortedRoot() {
        if (rootWritten) {
            return; // the real root was written; the happy path already finalized it
        }
        final TikaDocument root = seenRoot;
        if (root == null || root.isDuplicate() || writtenEmbeds.get() == 0) {
            return;
        }
        try {
            if (rootStubWritten) {
                spewer.finalizeAbortedRoot(root, writtenEmbeds.get());
            } else {
                rootStubWritten = spewer.writeRootStub(root, writtenEmbeds.get());
            }
        } catch (final Throwable t) {
            logger.error("failed to finalize/stub aborted root {}", root.getId(), t);
        }
    }
```

- [ ] **Step 6: Update the two existing normal-completion tests whose premise changed**

In `testFinalizesRootWithChildCountOnNormalCompletion`, delete the last assertion (the normal path now DOES write an early stub, so "no stub" is no longer true; stub coverage lives in the Step 1 test):

```java
        // DELETE this line:
        assertThat(spewer.rootStubs).isEmpty();
```

Delete the whole `testDoesNotWriteRootStubOnNormalCompletion` test (lines 174-189): its premise (no stub on normal completion) is exactly what root-stub-first reverses; the Step 1 test replaces it.

- [ ] **Step 7: Replace the abort test to assert the count refresh**

Replace `testWritesRootStubWhenParseAbortsBeforeRootIsWritten` (lines 152-172) with:

```java
    @Test
    public void testAbortAfterEarlyStubRefreshesChildCountKeepingPartial() throws Exception {
        RecordingSpewer spewer = new RecordingSpewer();
        TikaDocument root = doc("root", new StringReader("root-body"));
        TikaDocument e1 = doc("e1", new StringReader("a"));

        try (StreamingSpewCoordinator coord = new StreamingSpewCoordinator(spewer, 16)) {
            // Worker starts before the parse; the parse aborts (timeout/cancel) BEFORE the foreground
            // writes the root: spew(root) is never called, close() runs via try-with-resources.
            coord.start();
            coord.promise();
            coord.ready(new SpewItem(e1, root, root, 1));
        }

        // The early stub made the root visible at the first child (count 0 at that point). On abort the
        // real root was never written, so close() refreshes the count on the still-PARTIAL stub with the
        // final drained value (1) rather than writing a second stub.
        assertThat(spewer.written).contains("e1");
        assertThat(spewer.rootStubs).containsOnly("root");
        assertThat(spewer.rootStubChildCounts).containsOnly(0L);
        assertThat(spewer.finalizedAbortedRoots).containsOnly("root");
        assertThat(spewer.finalizeAbortedChildCounts).containsOnly(1L);
    }
```

- [ ] **Step 8: Add the reindex/no-clobber fallback test**

Add:

```java
    @Test
    public void testAbortDoesNotRefreshCountWhenEarlyStubReportedRootAlreadyExists() throws Exception {
        RecordingSpewer spewer = new RecordingSpewer();
        spewer.rootStubExists = true; // e.g. a prior COMPLETE root already indexed (reindex): stub is a no-op
        TikaDocument root = doc("root", new StringReader("root-body"));
        TikaDocument e1 = doc("e1", new StringReader("a"));

        try (StreamingSpewCoordinator coord = new StreamingSpewCoordinator(spewer, 16)) {
            coord.start();
            coord.promise();
            coord.ready(new SpewItem(e1, root, root, 1));
            // parse aborts before the foreground writes the root: spew() never called.
        }

        // writeRootStub reported "not written" (root already exists), so rootStubWritten stayed false and
        // the abort path must NOT call finalizeAbortedRoot -- refreshing the count would regress a prior
        // COMPLETE root. It falls back to writeRootStub, itself a no-op against the existing root.
        assertThat(spewer.finalizedAbortedRoots).isEmpty();
        assertThat(spewer.rootStubs).contains("root"); // attempted early + fallback, both no-ops
    }
```

- [ ] **Step 9: Run the whole coordinator test class, expect all green**

Run: `mvn -q -pl extract-lib test -Dtest=StreamingSpewCoordinatorTest`
Expected: PASS (all tests, including the untouched abort/duplicate/error tests, which do not assert on stubs).

- [ ] **Step 10: Commit**

```bash
git add extract-lib/src/main/java/org/icij/spewer/StreamingSpewCoordinator.java extract-lib/src/test/java/org/icij/spewer/StreamingSpewCoordinatorTest.java
git commit -m "feat(spew): write the container root stub at the first child (root-stub-first)"
```

---

## Task 3 (datashare, GATED on extract release): Elasticsearch stub/abort behavior + version bump

Do this in the datashare repo on a branch `feat/root-stub-first`, ONLY after the maintainer releases an extract version carrying Tasks 1+2. The version bump and the override signature change must land together (an old `void` override will not compile against the new `boolean` Spewer).

**Files:**
- Modify: `pom.xml:99` (`extract.version`)
- Modify: `datashare-index/src/main/java/org/icij/datashare/text/indexing/elasticsearch/ElasticsearchSpewer.java:155` (writeRootStub) and after line 207 (new finalizeAbortedRoot)
- Test: `datashare-index/src/test/java/org/icij/datashare/text/indexing/elasticsearch/ElasticsearchSpewerTest.java`

**Interfaces:**
- Consumes: `Spewer.writeRootStub(...) -> boolean`, `Spewer.finalizeAbortedRoot(...)` from the released extract-lib.
- Produces: `public boolean writeRootStub(...)`, `public void finalizeAbortedRoot(TikaDocument root, long writtenChildren)`.

- [ ] **Step 1: Create the datashare branch**

```bash
git switch -c feat/root-stub-first master
```

- [ ] **Step 2: Bump extract.version to the released version**

In `pom.xml` line 99, set `<extract.version>` to the extract release that contains Tasks 1+2 (expected `9.16.0`; confirm the actual released tag with the maintainer before committing).

```xml
        <extract.version>9.16.0</extract.version>
```

- [ ] **Step 3: Change writeRootStub to return boolean**

In `ElasticsearchSpewer.java`, change the signature at line 155 to `public boolean writeRootStub(TikaDocument root, long writtenChildren) throws IOException` and return from each branch. Replace the two skip `return;` statements (lines 163 and 171) with `return false;`, and add `return true;` at the end of the method (after the `logger.warn(...)` at line 188). The method body otherwise stays identical:

```java
    @Override
    public boolean writeRootStub(TikaDocument root, long writtenChildren) throws IOException {
        // ... unchanged comment ...
        if (root.isDuplicate()) {
            return false;
        }
        // ... unchanged no-clobber comment ...
        if (isDuplicate(root.getId())) {
            logger.debug("aborted parse: root {} already indexed, keeping it; not writing PARTIAL stub", shorten(root.getId(), 4));
            return false;
        }
        // ... unchanged stub-building code through indexer.add(indexName, document); ...
        logger.warn("aborted parse: wrote PARTIAL root stub {} with {} indexed child(ren): {}",
                shorten(root.getId(), 4), writtenChildren, root);
        return true;
    }
```

- [ ] **Step 4: Write the failing tests**

Add to `ElasticsearchSpewerTest.java` (mirroring the existing stub tests around line 81):

```java
    @Test
    public void test_write_root_stub_returns_true_on_first_write_and_false_when_root_exists() throws Exception {
        final TikaDocument root = new DocumentFactory().withIdentifier(new PathIdentifier()).create(get("early-stub.ost"));

        assertThat(spewer.writeRootStub(root, 1L)).isTrue();  // first write: a stub row was indexed
        assertThat(spewer.writeRootStub(root, 2L)).isFalse(); // root now exists: no-op, no clobber
    }

    @Test
    public void test_finalize_aborted_root_refreshes_count_and_keeps_partial() throws Exception {
        // The early stub was written PARTIAL with an initial count; the parse then aborted.
        final TikaDocument root = new DocumentFactory().withIdentifier(new PathIdentifier()).create(get("aborted-after-stub.ost"));
        spewer.writeRootStub(root, 0L);

        spewer.finalizeAbortedRoot(root, 4210L);

        GetResponse<ObjectNode> documentFields = es.client.get(doc -> doc.index(es.getIndexName()).id(root.getId()), ObjectNode.class);
        Map<String, Object> source = nodeToMap(documentFields.source());
        assertThat(source.get("recoveryStatus")).isEqualTo("PARTIAL"); // NOT complete: the parse did not finish
        assertThat(((Number) source.get("nbChildrenEmitted")).intValue()).isEqualTo(4210);
        assertThat(source.get("content")).isEqualTo(""); // still contentless
    }
```

- [ ] **Step 5: Run them, expect failure**

Run: `mvn -q -pl datashare-index -am test -Dtest=ElasticsearchSpewerTest#test_finalize_aborted_root_refreshes_count_and_keeps_partial+test_write_root_stub_returns_true_on_first_write_and_false_when_root_exists -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL (`finalizeAbortedRoot` is the inherited no-op, so `nbChildrenEmitted` is not updated / the doc has no such field).

- [ ] **Step 6: Implement finalizeAbortedRoot**

In `ElasticsearchSpewer.java`, add after `finalizeRoot` (after line 207):

```java

    @Override
    public void finalizeAbortedRoot(TikaDocument root, long writtenChildren) throws IOException {
        // The parse aborted after the early stub was written but before the real root write. Refresh the
        // child count on the still-PARTIAL stub via a partial update, WITHOUT touching recoveryStatus, so
        // the root stays PARTIAL (the container was not fully processed). A later re-run replaces the stub
        // with the fully-parsed root.
        Map<String, Object> fields = new HashMap<>();
        fields.put("nbChildrenEmitted", (int) Math.min(writtenChildren, Integer.MAX_VALUE));
        indexer.update(indexName, root.getId(), fields);
        logger.warn("aborted parse: refreshed PARTIAL root stub {} child count to {}: {}",
                shorten(root.getId(), 4), writtenChildren, root);
    }
```

- [ ] **Step 7: Run the tests, expect green**

Run: `mvn -q -pl datashare-index -am test -Dtest=ElasticsearchSpewerTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS (all `ElasticsearchSpewerTest` tests, including the existing stub/finalize tests which call `writeRootStub` as statements and are unaffected by the boolean return). Requires live ES reachable at `elasticsearch:9200`.

- [ ] **Step 8: Commit**

```bash
git add pom.xml datashare-index/src/main/java/org/icij/datashare/text/indexing/elasticsearch/ElasticsearchSpewer.java datashare-index/src/test/java/org/icij/datashare/text/indexing/elasticsearch/ElasticsearchSpewerTest.java
git commit -m "feat(index): root-stub-first support (writeRootStub returns written, finalizeAbortedRoot)"
```

---

## Self-Review

**Spec coverage:**
- "before the first child" hook -> Task 2 Step 3 (`writeEarlyRootStub` at first-child capture in run()). ✓
- boolean writeRootStub + finalizeAbortedRoot API -> Task 1. ✓
- coordinator early stub + abort count refresh + fallback -> Task 2 Steps 3, 5. ✓
- datashare writeRootStub boolean + finalizeAbortedRoot keeps PARTIAL -> Task 3 Steps 3, 6. ✓
- content-not-permanently-contentless (same id+path overwrite) -> covered by existing datashare behavior; asserted indirectly by `test_finalize_aborted_root...content == ""` staying PARTIAL and by the existing `test_write_root_stub_does_not_overwrite_an_already_indexed_root`; the real-write-overwrites-stub path is exercised in production by writeDocument(root) and by existing `test_finalize_root_marks_complete...preserving_content`. ✓
- PST rollup preserved -> unchanged `finalizeRoot`; `finalizeAbortedRoot` never sets status. Existing `test_finalize_root_does_not_override_pst_recovery_status` still guards finalize. ✓
- reindex no-clobber -> Task 2 Step 8 + datashare Task 3 Step 4 first assertion. ✓
- extract.version bump -> Task 3 Step 2. ✓
- forward-compat (single status, count as anchor, no reserved field) -> honored by construction (no new status, no new field). ✓

**Placeholder scan:** No TBD/TODO. The only deferred value is the exact released extract version in Task 3 Step 2, which is explicitly gated and instructed to confirm with the maintainer.

**Type consistency:** `writeRootStub` returns `boolean` in Spewer (Task 1), coordinator (Task 2 assigns to `rootStubWritten`), and datashare override (Task 3). `finalizeAbortedRoot(TikaDocument, long)` signature identical across Spewer, fixture, coordinator call, and datashare override. `rootStubWritten` field name consistent. ✓
