# OST-2013 Content-Layer Follow-ups (F3 / F4 / F5) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the three content-layer follow-ups from the OST-2013 robustness memo — correct folder-path attribution for orphan-recovered messages (F3), suppress java-libpst's un-loggable stdout/stderr noise (F4), and the codeable slice of the content/OCR pass plus a measurement runbook (F5).

**Architecture:** All extract-lib code on branch `feat/ost-2013-attachment-recovery`. F3 fixes attribution through java-libpst's *public* descriptor tree (`DescriptorIndexNode.parentDescriptorIndexIdentifier`) via a same-package `com.pff` helper — never touching the broken stateful `getNextChild`. F4 installs a thread-scoped filtering `PrintStream` over `System.out`/`System.err` once, gated by a `ThreadLocal` the parser sets around the PST parse. F5 demotes PDFBox font-noise via logback config in extract-cli, documents the encrypted-attachment residual, and ships a runbook for the deferred full-index measurement.

**Tech Stack:** Java, Maven multi-module (`extract-lib`, `extract-cli`), JUnit 4 (`org.junit.Test`), FEST assertions (`org.fest.assertions.Assertions.assertThat`), java-libpst 0.9.3 (`com.pff`, frozen/unmodified), Apache Tika 3.3.0, logback.

## Global Constraints

- **No reflection, no `--add-opens`, no fork of java-libpst.** Touch java-libpst internals only through same-package `com.pff` source classes reaching package-public members at compile time (the F1 mechanism, used by `com.pff.OstCompressedBlockReader` and `com.pff.PstMessageDescriptors`). `currentEmailIndex`/`emailsTable`/`initEmailsTable` are `private` and are out of bounds.
- **Attribution only, no count change.** F3 must not alter `PST_EXPECTED`/`PST_EMITTED`/`PST_FAILED` or the emitted message count for any PST — it only changes the folder path stamped on orphan-recovered messages.
- **Isolation pattern.** Every java-libpst accessor that can throw on a corrupt PST is wrapped so one failure never aborts the surrounding traversal; catch `Exception | LinkageError`, fall back. Mirror the existing `safe(...)` usage in `ResilientOutlookPSTParser`.
- **Conventional commits**, scoped `(pst)` / `(cli)`, matching branch history (`feat(pst):`, `test(pst):`, `build(cli):`, `docs(pst):`).
- **Test fixture:** `extract-lib/src/test/resources/documents/pst/testPST.pst` — a classic (non-type-36) Unicode PST with exactly 7 messages, no orphans. `getNextChild` does not fail on it, so it is the deterministic equivalence baseline, not an orphan-recovery reproduction.
- **Maven test invocation** (from the repo root): `mvn -pl extract-lib test -Dtest=<Class>#<method>` for a single test; `mvn -pl extract-lib test` for the module.

---

### Task 1: F3 — `com.pff.PstFolderPathResolver` (descriptor-tree folder paths)

Build the helper that computes each folder's full path from the public descriptor tree, producing strings identical to `ResilientOutlookPSTParser`'s folder walk. This is the correctness core of F3; the equivalence test against the walk is the gate.

**Files:**
- Create: `extract-lib/src/main/java/com/pff/PstFolderPathResolver.java`
- Test: `extract-lib/src/test/java/com/pff/PstFolderPathResolverTest.java`

**Interfaces:**
- Consumes: `PSTFile.getChildDescriptorTree()` (package-private → resolver must live in `com.pff`); `DescriptorIndexNode.descriptorIdentifier` (public int), `DescriptorIndexNode.parentDescriptorIndexIdentifier` (public int); `PSTObject.getNodeType(int)` (public static), `PSTObject.NID_TYPE_NORMAL_FOLDER` (public), `PSTObject.detectAndLoadPSTObject(PSTFile, long)` (public), `PSTFolder.getDisplayName()` (public), `PSTFile.getRootFolder().getDescriptorNodeId()` (public).
- Produces: `public static Map<Integer, String> folderPaths(PSTFile pst) throws IOException, PSTException` — maps each folder descriptor id to its full path (`"/"`, `"/Top of Personal Folders"`, `"/Top of Personal Folders/Inbox"`). Root maps to `"/"`. Unreadable folder name → segment `"?"` (matching the walk's `safeDisplayName`).

- [ ] **Step 1: Write the failing test**

Create `extract-lib/src/test/java/com/pff/PstFolderPathResolverTest.java`. The test proves the resolver computes, for every visible message, the same folder path the parser's walk emits — so an orphan-recovered message would get its true path.

```java
package com.pff;

import org.apache.tika.extractor.EmbeddedDocumentExtractor;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.PST;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.icij.extract.parser.ResilientOutlookPSTParser;
import org.junit.Test;
import org.xml.sax.ContentHandler;

import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.fest.assertions.Assertions.assertThat;

public class PstFolderPathResolverTest {

    private Path testPst() throws Exception {
        return Paths.get(getClass().getResource("/documents/pst/testPST.pst").toURI());
    }

    // Captures the folder path stamped on every emitted pst-mail-item by the parser's walk.
    private static final class FolderPathCapturingExtractor implements EmbeddedDocumentExtractor {
        final List<String> folderPaths = new ArrayList<>();

        public boolean shouldParseEmbedded(Metadata metadata) {
            return true;
        }

        public void parseEmbedded(InputStream stream, ContentHandler handler, Metadata metadata, boolean outputHtml) {
            String type = metadata.get(TikaCoreProperties.CONTENT_TYPE_PARSER_OVERRIDE);
            if (type != null && type.contains("pst-mail-item")) {
                folderPaths.add(metadata.get(PST.PST_FOLDER_PATH));
            }
        }
    }

    @Test
    public void resolverComputesSameFolderPathsAsTheWalk() throws Exception {
        // Ground truth: the paths the parser's folder walk actually emits.
        ResilientOutlookPSTParser parser = new ResilientOutlookPSTParser();
        FolderPathCapturingExtractor capture = new FolderPathCapturingExtractor();
        ParseContext context = new ParseContext();
        context.set(Parser.class, parser);
        context.set(EmbeddedDocumentExtractor.class, capture);
        Metadata metadata = new Metadata();
        try (InputStream is = TikaInputStream.get(testPst(), metadata)) {
            parser.parse(is, new BodyContentHandler(-1), metadata, context);
        }

        // Resolver-computed path for each visible message: folderPaths[message.parent].
        PSTFile pst = new PSTFile(testPst().toString());
        try {
            Map<Integer, String> folderPaths = PstFolderPathResolver.folderPaths(pst);
            List<String> resolved = new ArrayList<>();
            for (LinkedList<DescriptorIndexNode> nodes : pst.getChildDescriptorTree().values()) {
                for (DescriptorIndexNode node : nodes) {
                    if (PSTObject.getNodeType(node.descriptorIdentifier) == PSTObject.NID_TYPE_NORMAL_MESSAGE) {
                        String path = folderPaths.get(node.parentDescriptorIndexIdentifier);
                        if (path != null) {
                            resolved.add(path);
                        }
                    }
                }
            }

            List<String> expected = new ArrayList<>(capture.folderPaths);
            Collections.sort(expected);
            Collections.sort(resolved);
            assertThat(resolved).isEqualTo(expected);
            assertThat(resolved).hasSize(7);
        } finally {
            pst.getFileHandle().close();
        }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl extract-lib test -Dtest=PstFolderPathResolverTest#resolverComputesSameFolderPathsAsTheWalk`
Expected: compilation failure — `cannot find symbol: class PstFolderPathResolver`.

- [ ] **Step 3: Write minimal implementation**

Create `extract-lib/src/main/java/com/pff/PstFolderPathResolver.java`:

```java
package com.pff;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

/**
 * Helper placed in package {@code com.pff} so it can read java-libpst's
 * package-private descriptor tree. Computes each folder's full display path
 * ("/", "/Inbox", "/Inbox/Sub") from the descriptor tree's public parent links,
 * producing strings identical to {@code ResilientOutlookPSTParser}'s folder walk.
 *
 * <p>This lets orphan recovery stamp a recovered message with its true folder
 * path instead of the "/[recovered]" sentinel, without touching java-libpst's
 * broken stateful {@code PSTFolder.getNextChild()}.
 */
public final class PstFolderPathResolver {

    // Matches ResilientOutlookPSTParser.safeDisplayName: an unreadable folder name becomes "?".
    private static final String UNREADABLE_NAME = "?";

    private PstFolderPathResolver() {
    }

    public static Map<Integer, String> folderPaths(final PSTFile pst) throws IOException, PSTException {
        final int rootId = (int) pst.getRootFolder().getDescriptorNodeId();

        // Collect every folder descriptor's parent link (from the node, no load needed) and
        // its display name (needs a load, isolated so one bad folder degrades to "?").
        final Map<Integer, Integer> parentOf = new HashMap<>();
        final Map<Integer, String> nameOf = new HashMap<>();
        for (final LinkedList<DescriptorIndexNode> nodes : pst.getChildDescriptorTree().values()) {
            for (final DescriptorIndexNode node : nodes) {
                if (PSTObject.getNodeType(node.descriptorIdentifier) != PSTObject.NID_TYPE_NORMAL_FOLDER) {
                    continue;
                }
                parentOf.put(node.descriptorIdentifier, node.parentDescriptorIndexIdentifier);
                nameOf.put(node.descriptorIdentifier, displayName(pst, node.descriptorIdentifier));
            }
        }

        final Map<Integer, String> paths = new HashMap<>();
        for (final Integer folderId : parentOf.keySet()) {
            paths.put(folderId, buildPath(folderId, rootId, parentOf, nameOf));
        }
        paths.put(rootId, "/");
        return paths;
    }

    // Walks parent links from this folder up to root, collecting names root-first. Stops at
    // root, at a parent that is not a known folder (partial path from resolvable ancestors),
    // or on a cycle (corrupt tree). Mirrors the walk's "/"-join so strings match exactly.
    private static String buildPath(final int folderId, final int rootId,
                                    final Map<Integer, Integer> parentOf, final Map<Integer, String> nameOf) {
        final Deque<String> names = new ArrayDeque<>();
        final Set<Integer> seen = new HashSet<>();
        int current = folderId;
        while (current != rootId && parentOf.containsKey(current) && seen.add(current)) {
            names.push(nameOf.getOrDefault(current, UNREADABLE_NAME));
            current = parentOf.get(current);
        }
        if (names.isEmpty()) {
            return "/";
        }
        return "/" + String.join("/", names);
    }

    private static String displayName(final PSTFile pst, final int descriptorId) {
        try {
            final PSTObject object = PSTObject.detectAndLoadPSTObject(pst, (long) descriptorId);
            final String name = (object instanceof PSTFolder) ? ((PSTFolder) object).getDisplayName() : null;
            return (name == null || name.isEmpty()) ? UNREADABLE_NAME : name;
        } catch (final Exception | LinkageError e) {
            return UNREADABLE_NAME;
        }
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -pl extract-lib test -Dtest=PstFolderPathResolverTest#resolverComputesSameFolderPathsAsTheWalk`
Expected: PASS. (If it fails on exact strings, the parser walk's root differs from `getRootFolder()`'s descriptor id — reconcile by confirming `rootId` is the id the visible folders' parent chain terminates at; do not change the walk.)

- [ ] **Step 5: Commit**

```bash
git add extract-lib/src/main/java/com/pff/PstFolderPathResolver.java extract-lib/src/test/java/com/pff/PstFolderPathResolverTest.java
git commit -m "$(cat <<'EOF'
feat(pst): resolve true folder paths from the descriptor tree

Compute each folder's full path from DescriptorIndexNode's public parent
links, producing strings identical to the folder walk. Enables orphan
recovery to attribute recovered messages to their real folder instead of
the /[recovered] sentinel, with no reflection and no getNextChild change.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: F3 — Wire the resolver into orphan recovery

Use the resolver in `recoverOrphan` so recovered messages get their real folder path; keep `/[recovered]` only as the fallback when the parent is unresolvable.

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/extract/parser/ResilientOutlookPSTParser.java`
- Test: `extract-lib/src/test/java/org/icij/extract/parser/ResilientOutlookPSTParserTest.java`

**Interfaces:**
- Consumes: `com.pff.PstFolderPathResolver.folderPaths(PSTFile)` (Task 1); `PSTObject.getDescriptorNode().parentDescriptorIndexIdentifier` (public).
- Produces: `static String folderPathOrRecovered(Map<Integer, String> folderPaths, int parentId)` — returns the mapped path, or `RECOVERED_FOLDER_PATH` when absent. Package-private static for direct unit testing.

- [ ] **Step 1: Write the failing test**

Add to `ResilientOutlookPSTParserTest.java` (imports `java.util.HashMap`, `java.util.Map`):

```java
    @Test
    public void folderPathOrRecovered_returnsMappedPath_orRecoveredFallback() {
        Map<Integer, String> folderPaths = new HashMap<>();
        folderPaths.put(42, "/Top of Personal Folders/Inbox");

        // Resolvable parent -> its real folder path.
        assertThat(ResilientOutlookPSTParser.folderPathOrRecovered(folderPaths, 42))
                .isEqualTo("/Top of Personal Folders/Inbox");
        // Unresolvable parent -> the recovered sentinel.
        assertThat(ResilientOutlookPSTParser.folderPathOrRecovered(folderPaths, 99))
                .isEqualTo("/[recovered]");
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl extract-lib test -Dtest=ResilientOutlookPSTParserTest#folderPathOrRecovered_returnsMappedPath_orRecoveredFallback`
Expected: compilation failure — `cannot find symbol: method folderPathOrRecovered`.

- [ ] **Step 3: Write minimal implementation**

In `ResilientOutlookPSTParser.java`:

(a) Add imports near the existing `com.pff` imports:
```java
import com.pff.PstFolderPathResolver;
import java.util.Map;
import java.util.Collections;
```

(b) In `extractAllMessages`, build the folder-path map once and thread it into orphan recovery. Replace:
```java
        final List<Integer> messageDescriptorIds = enumerateMessageDescriptorIds(pstFile, pstPath);
        walkFolder(pstFile.getRootFolder(), "/", emission);
        if (messageDescriptorIds != null) {
            recoverOrphans(messageDescriptorIds, pstFile, emission);
        }
```
with:
```java
        final List<Integer> messageDescriptorIds = enumerateMessageDescriptorIds(pstFile, pstPath);
        walkFolder(pstFile.getRootFolder(), "/", emission);
        if (messageDescriptorIds != null) {
            recoverOrphans(messageDescriptorIds, pstFile, resolveFolderPaths(pstFile, pstPath), emission);
        }
```

(c) Add the folder-path map builder (isolated: a failure to build it must not abort the parse, so orphan recovery falls back to `/[recovered]`):
```java
    // Builds the descriptor-tree folder-path map used to attribute orphan-recovered messages.
    // Returns an empty map on failure so recovery degrades to the /[recovered] sentinel rather
    // than aborting -- attribution is best-effort, never load-bearing for emission.
    private Map<Integer, String> resolveFolderPaths(final PSTFile pstFile, final String pstPath) {
        try {
            return PstFolderPathResolver.folderPaths(pstFile);
        } catch (final Exception | LinkageError e) {
            logger.warn("Could not resolve PST folder paths for \"{}\"; recovered messages will use {}.",
                    pstPath, RECOVERED_FOLDER_PATH, e);
            return Collections.emptyMap();
        }
    }
```

(d) Change `recoverOrphans`/`recoverOrphan` to take the map and compute the path:
```java
    private void recoverOrphans(final List<Integer> messageDescriptorIds, final PSTFile pstFile,
                                final Map<Integer, String> folderPaths, final EmissionContext emission) {
        for (final int descriptorNodeId : messageDescriptorIds) {
            recoverOrphan(descriptorNodeId, pstFile, folderPaths, emission);
        }
    }

    private void recoverOrphan(final int descriptorNodeId, final PSTFile pstFile,
                               final Map<Integer, String> folderPaths, final EmissionContext emission) {
        final long descriptorId = descriptorNodeId;
        if (emission.alreadyEmitted(descriptorId)) {
            return;
        }
        try {
            final PSTObject object = PSTObject.detectAndLoadPSTObject(pstFile, descriptorId);
            if (object instanceof PSTMessage) {
                final int parentId = object.getDescriptorNode().parentDescriptorIndexIdentifier;
                emitMessage((PSTMessage) object, folderPathOrRecovered(folderPaths, parentId), emission);
            }
        } catch (final Exception | LinkageError e) {
            logger.debug("PST orphan descriptor {} is not a loadable message; skipping.", descriptorNodeId, e);
        }
    }
```

(e) Add the testable helper:
```java
    // Maps a recovered message's parent folder descriptor id to its real folder path, falling
    // back to the recovered sentinel when the parent is not a resolvable folder.
    static String folderPathOrRecovered(final Map<Integer, String> folderPaths, final int parentId) {
        return folderPaths.getOrDefault(parentId, RECOVERED_FOLDER_PATH);
    }
```

- [ ] **Step 4: Run the test and the full parser suite to verify pass + no regression**

Run: `mvn -pl extract-lib test -Dtest=ResilientOutlookPSTParserTest`
Expected: PASS — including the pre-existing `test_emits_every_message_and_sets_reconciliation_metadata` (7/7/0) and `test_extractor_pipeline_uses_resilient_parser`, proving counts and existing behavior are unchanged (attribution-only).

- [ ] **Step 5: Commit**

```bash
git add extract-lib/src/main/java/org/icij/extract/parser/ResilientOutlookPSTParser.java extract-lib/src/test/java/org/icij/extract/parser/ResilientOutlookPSTParserTest.java
git commit -m "$(cat <<'EOF'
feat(pst): attribute orphan-recovered messages to their real folder

Orphan recovery now resolves each recovered message's parent folder
through the descriptor tree, stamping its true path instead of the
/[recovered] sentinel; the sentinel remains only when the parent is
unresolvable. Attribution-only: emitted counts and reconciliation are
unchanged.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

---

### Task 3: F4 — Thread-scoped stdout/stderr noise filter

Suppress java-libpst's raw `println` output (it bypasses slf4j) only on threads actively parsing a PST, installed once globally and transparent to everything else.

**Files:**
- Create: `extract-lib/src/main/java/org/icij/extract/parser/PstStdoutFilter.java`
- Modify: `extract-lib/src/main/java/org/icij/extract/parser/ResilientOutlookPSTParser.java`
- Test: `extract-lib/src/test/java/org/icij/extract/parser/PstStdoutFilterTest.java`

**Interfaces:**
- Produces:
  - `static PrintStream suppressible(OutputStream delegate, java.util.function.BooleanSupplier suppress)` — a `PrintStream` that drops all writes while `suppress.getAsBoolean()` is true and passes everything else through to `delegate`. Package-private, for direct testing.
  - `static void install()` — idempotent; wraps `System.out`/`System.err` once, gated by the thread flag.
  - `static void begin()` / `static void end()` — set/clear the current thread's "inside PST parse" flag.

- [ ] **Step 1: Write the failing test**

Create `extract-lib/src/test/java/org/icij/extract/parser/PstStdoutFilterTest.java`:

```java
package org.icij.extract.parser;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.fest.assertions.Assertions.assertThat;

public class PstStdoutFilterTest {

    @Test
    public void suppressible_dropsWritesWhileFlagged_passesThroughOtherwise() {
        ByteArrayOutputStream sink = new ByteArrayOutputStream();
        boolean[] suppress = {false};
        PrintStream stream = PstStdoutFilter.suppressible(sink, () -> suppress[0]);

        stream.println("keep me");
        suppress[0] = true;
        stream.println("Unknown message type: IPM.AbchPerson"); // java-libpst noise
        suppress[0] = false;
        stream.println("keep again");
        stream.flush();

        String out = sink.toString();
        assertThat(out).contains("keep me");
        assertThat(out).contains("keep again");
        assertThat(out).excludes("Unknown message type");
    }

    @Test
    public void beginEnd_toggleTheCurrentThreadFlag() {
        ByteArrayOutputStream sink = new ByteArrayOutputStream();
        PrintStream stream = PstStdoutFilter.suppressible(sink, PstStdoutFilter::isSuppressingCurrentThread);

        stream.println("before begin");
        PstStdoutFilter.begin();
        try {
            stream.println("during parse");
        } finally {
            PstStdoutFilter.end();
        }
        stream.println("after end");
        stream.flush();

        String out = sink.toString();
        assertThat(out).contains("before begin");
        assertThat(out).contains("after end");
        assertThat(out).excludes("during parse");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -pl extract-lib test -Dtest=PstStdoutFilterTest`
Expected: compilation failure — `cannot find symbol: class PstStdoutFilter`.

- [ ] **Step 3: Write minimal implementation**

Create `extract-lib/src/main/java/org/icij/extract/parser/PstStdoutFilter.java`:

```java
package org.icij.extract.parser;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.function.BooleanSupplier;

/**
 * Suppresses java-libpst's direct {@code System.out}/{@code System.err} output (raw
 * {@code println} that bypasses slf4j, so logback cannot demote it) on threads that are
 * actively parsing a PST. Installed once over the JVM streams; transparent pass-through
 * for every other thread, so the rest of the process is unaffected.
 *
 * <p>Our own diagnostics go through slf4j to a different appender, so the recovery and
 * reconciliation signal survives while the library noise is dropped.
 */
final class PstStdoutFilter {

    private static final ThreadLocal<Boolean> IN_PARSE = ThreadLocal.withInitial(() -> Boolean.FALSE);
    private static boolean installed = false;

    private PstStdoutFilter() {
    }

    // Wraps System.out/System.err exactly once. Idempotent and synchronized: repeated parses
    // and multiple parser instances never stack wrappers.
    static synchronized void install() {
        if (installed) {
            return;
        }
        System.setOut(suppressible(System.out, PstStdoutFilter::isSuppressingCurrentThread));
        System.setErr(suppressible(System.err, PstStdoutFilter::isSuppressingCurrentThread));
        installed = true;
    }

    static void begin() {
        IN_PARSE.set(Boolean.TRUE);
    }

    static void end() {
        IN_PARSE.remove();
    }

    static boolean isSuppressingCurrentThread() {
        return IN_PARSE.get();
    }

    // A PrintStream whose every byte funnels through a conditional FilterOutputStream: when the
    // supplier says "suppress", the bytes are dropped; otherwise they reach the delegate. Gating
    // at the OutputStream layer (not by overriding PrintStream's text methods) is the reliable
    // interception point, since PrintStream always routes encoded bytes to its underlying stream.
    static PrintStream suppressible(final OutputStream delegate, final BooleanSupplier suppress) {
        final OutputStream gate = new FilterOutputStream(delegate) {
            @Override
            public void write(final int b) throws IOException {
                if (!suppress.getAsBoolean()) {
                    out.write(b);
                }
            }

            @Override
            public void write(final byte[] b, final int off, final int len) throws IOException {
                if (!suppress.getAsBoolean()) {
                    out.write(b, off, len);
                }
            }
        };
        return new PrintStream(gate, true);
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -pl extract-lib test -Dtest=PstStdoutFilterTest`
Expected: PASS (both tests).

- [ ] **Step 5: Wire into the parser**

In `ResilientOutlookPSTParser.parse(...)`, install once and bracket the parse region. Replace:
```java
        xhtml.startDocument();
        final String pstPath = TikaInputStream.get(stream).getFile().getPath();

        PSTFile pstFile = null;
        try {
            pstFile = new PSTFile(pstPath);
            extractAllMessages(pstFile, pstPath, emission, metadata);
        } catch (final TikaException e) {
            throw e;
        } catch (final Exception e) {
            throw new TikaException(e.getMessage(), e);
        } finally {
            closeQuietly(pstFile);
        }

        xhtml.endDocument();
```
with:
```java
        xhtml.startDocument();
        final String pstPath = TikaInputStream.get(stream).getFile().getPath();

        PstStdoutFilter.install();
        PstStdoutFilter.begin();
        PSTFile pstFile = null;
        try {
            pstFile = new PSTFile(pstPath);
            extractAllMessages(pstFile, pstPath, emission, metadata);
        } catch (final TikaException e) {
            throw e;
        } catch (final Exception e) {
            throw new TikaException(e.getMessage(), e);
        } finally {
            closeQuietly(pstFile);
            PstStdoutFilter.end();
        }

        xhtml.endDocument();
```

- [ ] **Step 6: Run the parser suite to verify no regression**

Run: `mvn -pl extract-lib test -Dtest=ResilientOutlookPSTParserTest,PstStdoutFilterTest`
Expected: PASS. The filter is pass-through outside the parse region, so all existing assertions hold.

- [ ] **Step 7: Commit**

```bash
git add extract-lib/src/main/java/org/icij/extract/parser/PstStdoutFilter.java extract-lib/src/test/java/org/icij/extract/parser/PstStdoutFilterTest.java extract-lib/src/main/java/org/icij/extract/parser/ResilientOutlookPSTParser.java
git commit -m "$(cat <<'EOF'
feat(pst): suppress java-libpst stdout/stderr noise during PST parse

java-libpst prints ~1,100+ raw println lines per OST parse that bypass
slf4j and bury the recovery signal ~20:1. Install a thread-scoped
filtering PrintStream once over System.out/err, gated by a ThreadLocal
the parser sets around the parse; transparent pass-through everywhere
else. Our slf4j diagnostics are unaffected.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

---

### Task 4: F5a — Demote PDFBox font-noise in the CLI logback

Raise the PDFBox font/stream-engine loggers to ERROR so the ~95%-of-WARN-volume font-fallback spam (`No current font`, `SetFontAndSize`, glyph/TrueType fallbacks) stops burying real failures in the CLI harness. PDFBox 3.x (Tika 3.3.0) logs via slf4j, so package-level logback config silences it without a code change. Real PDFBox WARNs outside these loggers (encryption, structure) are preserved.

**Files:**
- Modify: `extract-cli/src/main/resources/logback.xml`

- [ ] **Step 1: Add the logger entries**

In `extract-cli/src/main/resources/logback.xml`, insert before `<root level="info" ...>`:
```xml
    <!-- PDFBox/FontBox font-fallback warnings are ~95% of OCR WARN volume and bury real
         failures (extract-indexing-robustness memo §8, F5). They originate inside PDFBox,
         so the only lever is logger level. Real PDFBox issues outside these names still log. -->
    <logger name="org.apache.pdfbox.contentstream.PDFStreamEngine" level="error"/>
    <logger name="org.apache.pdfbox.pdmodel.font" level="error"/>
    <logger name="org.apache.fontbox" level="error"/>
```

- [ ] **Step 2: Verify the config is well-formed and the module builds**

Run: `mvn -pl extract-cli -am compile`
Expected: BUILD SUCCESS (logback is parsed at runtime, but the build confirms the resource is in place and the XML is committed).
Then confirm the entries are present: `grep -n "PDFStreamEngine\|org.apache.fontbox" extract-cli/src/main/resources/logback.xml` → 2+ matches.

> Note: confirm the exact emitting logger names against the F5 measurement run (Task 5 runbook). If `No current font` is logged under a different name in the pinned PDFBox build, add that name here. The three above cover the names called out in the memo.

- [ ] **Step 3: Commit**

```bash
git add extract-cli/src/main/resources/logback.xml
git commit -m "$(cat <<'EOF'
build(cli): demote PDFBox font-fallback logs to error

Font-render warnings (No current font, glyph/TrueType fallbacks) are ~95%
of OCR WARN volume and bury real failures. Raise the PDFBox font and
stream-engine loggers to error; other PDFBox warnings still log. Runtime
logback in datashare gets the same treatment as a hand-off.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

---

### Task 5: F5b + F5d — Encrypted-residual doc + content/OCR measurement runbook

Document the encrypted-attachment residual as a distinct class, and ship the precise runbook for the deferred full-index content/OCR measurement (needs the 48 GB OST + an OCR environment with `<lang>` tessdata, which cannot run in this session).

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/extract/parser/ResilientOutlookPSTParser.java` (class javadoc only)
- Create: `docs/superpowers/runbooks/2026-06-25-ost-content-ocr-measurement.md`

- [ ] **Step 1: Add the encrypted-residual note to the parser javadoc**

In `ResilientOutlookPSTParser`'s class javadoc, after the existing paragraph about handing messages to `PSTMailItemParser`, add:
```java
 * <p><b>Encrypted-attachment residual.</b> An attachment whose bytes are recovered
 * byte-perfectly but are password-protected is indexed as an attachment, yet its body
 * text cannot be extracted without credentials -- a distinct residual class from a
 * reader defect, counted by {@link #PST_ATTACHMENTS_ENCRYPTED}. Note that under Tika's
 * default {@code ParsingEmbeddedDocumentExtractor} this counter reads 0 because that
 * extractor swallows {@code EncryptedDocumentException} internally; the recovered bytes
 * are emitted and indexed regardless. A password/credential extraction path is future
 * work, not handled here.
```

- [ ] **Step 2: Write the measurement runbook**

Create `docs/superpowers/runbooks/2026-06-25-ost-content-ocr-measurement.md`:

```markdown
# Runbook — OST-2013 content/OCR measurement pass (F5)

Status: to execute once a 48 GB OST and an OCR environment with eng+<lang>
tessdata are available. The big-OST validation run used a counting extractor, so the content
and OCR layer was never exercised; this run surfaces that layer's error profile.

## Prerequisites
- The 48 GB OST file on local disk (java-libpst reads on demand; ~570 MB RSS observed).
- Tesseract with `eng`, `osd`, and `<lang>` tessdata installed (<lang> closes the recall gap; see ocr-language-packages). <lang> install is a datashare-installer hand-off.
- extract built from branch `feat/ost-2013-attachment-recovery`.

## Invocation
- Real `Extractor` → SHA-256 → `ResilientOutlookPSTParser`, `embedHandling=spawn`, OCR `AUTO`, languages `eng+<lang>`, ergonomic heap, output to a real spewer (or `/dev/null` sink for insight-only).
- This differs from the prior run by using the real Tika parse + OCR + spewer rather than the counting extractor.

## Capture (per signal)
1. PDFBox parse-failure profile: counts by exception class (e.g. TIKA-198, encryption), distinct from the font-noise demoted in Task 4. Confirm Task 4's logger names actually silence `No current font` in this build; add any missed name.
2. OCR recall/quality on recovered JPEGs/PDFs with <lang> installed vs eng-only.
3. Encrypted-attachment behaviour: `pst_attachments_encrypted` (note the Tika-swallow caveat — it may read 0; cross-check by counting EncryptedDocumentException at the parse layer if a custom extractor is used).
4. Reconciliation: `pst_expected` / `pst_emitted` / `pst_failed` and `pst_attachments_unreadable/recovered/unrecovered/encrypted`. Expect parity with the prior counting run (~12,959 unreadable → ~11,547 recovered, 0 net message loss) since F3 is attribution-only.
5. Folder attribution: count of messages stamped `/[recovered]` — expect it to drop versus the pre-F3 baseline (F3 now attributes most recovered messages to their real folder).

## Pass/fail
- No new net message loss (`pst_expected == pst_emitted`).
- Font-noise no longer dominates WARN volume (Task 4 effective).
- <lang> materially improves OCR text on the non-English scans.
- `/[recovered]` attribution count strictly lower than the pre-F3 run.

## Hand-offs surfaced (not extract-lib code)
- datashare: index/expose `pst_*` metadata so reconciliation and the encrypted residual are queryable and alertable (F3 second half).
- datashare runtime logback: mirror Task 4's PDFBox logger demotion.
- datashare-installer: install `<lang>` tessdata per channel (apt / brew / snap stage-packages).
```

- [ ] **Step 3: Verify the module still builds (javadoc change)**

Run: `mvn -pl extract-lib -am compile`
Expected: BUILD SUCCESS.

- [ ] **Step 4: Commit**

```bash
git add extract-lib/src/main/java/org/icij/extract/parser/ResilientOutlookPSTParser.java docs/superpowers/runbooks/2026-06-25-ost-content-ocr-measurement.md
git commit -m "$(cat <<'EOF'
docs(pst): document encrypted-attachment residual + content/OCR runbook

Note the encrypted-attachment residual class (byte-perfect recovery,
body needs credentials) and the Tika-swallow counter caveat in the
parser javadoc. Add the runbook for the deferred full-index content/OCR
measurement (48 GB OST + eng+<lang> OCR), with capture points and hand-offs.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>
EOF
)"
```

> Note: `docs/superpowers/` is git-ignored in this repo (as with the F1 spec/plan). The runbook lives in-repo for reference; the `git add` above will be a no-op for the ignored path unless force-added. Force-add only if the team decides to track it.

---

## Hand-offs (not implemented in this plan — captured for the datashare/installer track)

These reach outside extract-lib and were scoped out during brainstorming (extract-lib only this session):

- **datashare — make `pst_*` queryable (F3 second half).** Index/expose `tika:pst_expected`, `tika:pst_emitted`, `tika:pst_failed`, and `tika:pst_attachments_unreadable/recovered/unrecovered/encrypted` on the ES document so reconciliation loss and the encrypted residual are queryable and alertable rather than only logged.
- **datashare — runtime logback (F5a).** Mirror Task 4's PDFBox/FontBox logger demotion in datashare's runtime logback config.
- **datashare-installer — `<lang>` tessdata (F5c).** Add the corpus language tessdata per packaging channel (apt `Depends`, Homebrew/MacPorts `tesseract-lang`, snap `stage-packages`), per the Tesseract precedent.

## Self-Review

- **Spec coverage:** F3 attribution → Tasks 1-2; F3 "queryable signal" → hand-off (per spec). F4 → Task 3. F5a → Task 4. F5b → Task 5 Step 1. F5c → hand-off. F5d → Task 5 Step 2. All spec sections mapped.
- **Placeholders:** none — every code/test/config step shows the full content; the two "Note" callouts are validation guidance against the deferred run, not deferred implementation.
- **Type consistency:** `folderPaths(PSTFile) -> Map<Integer,String>` (Task 1) consumed verbatim in Task 2; `folderPathOrRecovered(Map<Integer,String>, int)`, `suppressible(OutputStream, BooleanSupplier)`, `install()/begin()/end()/isSuppressingCurrentThread()` referenced consistently across tasks and tests.
```
