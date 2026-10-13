# Artifact Embed Depth Guard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the ARTIFACT / download re-extraction walk reach every embed the INDEX pass accepted, refuse exactly what INDEX refuses, and stop relying on Tika's SAX depth counters as the bound.

**Architecture:** Four independent changes in `extract-lib`. `EmbedParser` closes its package-entry wrapper in a `finally` so one failed embed cannot leak nesting onto its siblings. `DigestEmbeddedDocumentExtractor` gains an explicit nesting bound that reuses the constant and predicate INDEX already uses (`EmbedSpawner.DEFAULT_MAX_EMBED_DEPTH`, `EmbedSpawner.exceedsMaxEmbedDepth`) against the balanced `documentStack` it already maintains. `relaxZipBombGuard` stays but its comment is restated, since the counters it relaxes are no longer the protection. The untested `discardingHandler` swap gets a test.

**Tech Stack:** Java, Maven multi-module (`extract-lib`), Apache Tika 3.3.0, JUnit 4, fest-assert 1.4, slf4j.

## Global Constraints

- Spec: `docs/superpowers/specs/2026-07-26-artifact-embed-depth-guard-design.md`.
- Depth bound is `EmbedSpawner.DEFAULT_MAX_EMBED_DEPTH` (20). Never hardcode 20; reference the constant so the two paths share one definition of the default. Note this gives parity only at the default: `maxEmbedDepth` is an `Extractor` CLI option that overrides it at index time (`Extractor.java:150,319-324`), so a deployment that configures it makes retrieval refuse at a different level than INDEX did. Passing the effective depth into the guard is the real fix, out of scope here.
- Depth predicate is `EmbedSpawner.exceedsMaxEmbedDepth(stackSize, maxEmbedDepth)`. Do not write a second predicate.
- `maximumCompressionRatio` keeps its Tika default in every parser builder. Nothing in this plan touches it.
- Indentation follows each file: `EmbedParser.java` uses tabs, `EmbeddedDocumentExtractor.java` uses 4 spaces, test files use 4 spaces.
- Comments and test names describe mechanisms only. No project, corpus, box, or client identifiers anywhere.
- Test runner: `mvn -o -pl extract-lib test -Dtest=<ClassName>`. Surefire's per-class report lands in `extract-lib/target/surefire-reports/org.icij.extract.extractor.<ClassName>.txt`; read it when console output is quiet.
- Commit with the `/commit` skill, never a raw `git commit`.
- Branch: `fix/artifact-embed-depth-guard`. Its first commit, `c615e24 fix(artifacts): relax the retrieval walk's depth guard and stop buffering its text`, is the baseline: it carries `relaxZipBombGuard`, the `discardingHandler` swap, and the old zip-bomb depth test. Keep those changes; the only permitted revert is the temporary RED check in Task 2 Step 3.
- Known-failing before any of this work, on a clean tree, from the OCR/Tesseract environment rather than from these changes: `Tess4JOCRParserTest` (7 failures, 7 errors) and `ExtractorTest.test_ocr_confidence` (asserts confidence > 0, gets 0.0). Fifteen in total. Treat only NEW failures as regressions. A stale `FlowReproTest.txt` in `surefire-reports` has no source or class file behind it; ignore it.

## File Structure

**Modified:**
- `extract-lib/src/main/java/org/icij/extract/extractor/EmbedParser.java` - Task 1. `parseEmbedded` gains a `try/finally` around the delegate parse. Nothing else in the file changes.
- `extract-lib/src/main/java/org/icij/extract/extractor/EmbeddedDocumentExtractor.java` - Tasks 2, 3, 4. Depth guard at the top of `DigestEmbeddedDocumentExtractor.delegateParsing`; comment restated on `relaxZipBombGuard`; `discardingHandler` widened to package-private; two inline fully qualified names replaced by imports.

**Created:**
- `extract-lib/src/test/java/org/icij/extract/extractor/EmbedParserPackageEntryBalanceTest.java` - Task 1. Proves a failed embed does not leak package-entry depth onto its sibling, using Tika's real `SecureContentHandler` accounting.
- `extract-lib/src/test/java/org/icij/extract/extractor/EmbeddedDocumentExtractorEmbedDepthTest.java` - Task 2. Proves every level within the bound is cached and nothing past it is.
- `extract-lib/src/test/java/org/icij/extract/extractor/EmbeddedDocumentExtractorDiscardingHandlerTest.java` - Task 4. Proves the artifact handler does not accumulate body text.

**Deleted:**
- `extract-lib/src/test/java/org/icij/extract/extractor/EmbeddedDocumentExtractorZipBombDepthTest.java` - Task 2. Currently untracked and asserts at least 130 raw artifacts from a 130-level nested zip, which contradicts a 20-level bound. Its `nestedZip` helper moves into the new test.

## Background an implementer needs

Three mechanisms, all verified against Tika 3.3.0 sources, explain every step below.

`SecureContentHandler` keeps one `currentDepth` counter and one `packageEntryDepths` list for the whole root parse. `startElement` does `currentDepth++`, throws `SecureSAXException` if `currentDepth >= maxDepth`, and for a `<div class="package-entry">` pushes `currentDepth` then throws if `packageEntryDepths.size() >= maxPackageEntryDepth`. `endElement` pops only when `packageEntryDepths.getLast() == currentDepth`, compared before the decrement. `maxDepth` defaults to 100; `maxPackageEntryDepth` defaults to 10.

`SecureSAXException` extends `SAXException`, not `TikaException`, so `EmbedParser.delegateParsing`'s tolerant `catch (TikaException)` does not catch it and it propagates out of `parseEmbedded`.

`EmbedParser.writeStart` opens that div and `writeEnd` closes it, but `writeEnd` is not in a `finally` today. Callers that tolerate per-entry failures keep walking (for example `ResilientOutlookPSTParser.java:471-480`, which catches `Exception | LinkageError` per message), so each failed embed leaves the div open and the counters permanently elevated. Each embed level adds exactly one `<div class="package-entry">`, so it is the 10-level `packageEntryDepths` counter, not the 100-level `currentDepth` counter, that runs out first: past roughly nine failures every later embed is refused regardless of its real nesting.

Task 1 fixes the leak for our own div. It does not make Tika's counters trustworthy in general: when a delegate parser throws mid-document its own open elements are never closed either, so `currentDepth` is inflated by the time `writeEnd` fires and the pop condition fails. That is why Task 2 supplies the bound instead of tightening the counters back down.

---

### Task 1: Balanced package-entry emission in EmbedParser

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/extract/extractor/EmbedParser.java:57-68`
- Test: `extract-lib/src/test/java/org/icij/extract/extractor/EmbedParserPackageEntryBalanceTest.java` (create)

**Interfaces:**
- Consumes: nothing from other tasks.
- Produces: no signature changes. `EmbedParser.parseEmbedded(InputStream, ContentHandler, Metadata, boolean)` keeps its contract, including propagating a delegate `SAXException` to the caller. Later tasks rely only on the behavior that the wrapper div is always closed.

- [ ] **Step 1: Write the failing test**

Create `extract-lib/src/test/java/org/icij/extract/extractor/EmbedParserPackageEntryBalanceTest.java`:

```java
package org.icij.extract.extractor;

import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.SecureContentHandler;
import org.icij.extract.document.DigestIdentifier;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.TikaDocument;
import org.junit.Test;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;

import static org.fest.assertions.Assertions.assertThat;

/**
 * EmbedParser wraps every emitted embed in a nested <div class="package-entry">, and Tika's
 * SecureContentHandler counts those divs across the WHOLE root parse, popping an entry only on the
 * matching endElement. A per-embed parse failure that skips writeEnd therefore leaves the entry on
 * the stack forever, and since callers of this walk tolerate per-entry failures and keep going, the
 * nesting count climbs with every failure until the guard refuses embeds that are not nested at all.
 * This pins that one failed embed cannot cost its siblings any nesting budget.
 */
public class EmbedParserPackageEntryBalanceTest {

    private static final String DELEGATE_FAILURE = "embed parse failed";

    // Fails the way the zip-bomb guard fails: with a SAXException, which delegateParsing's tolerant
    // catch (TikaException) deliberately does not catch, so it propagates out of parseEmbedded.
    private static class FailingEmbedParser extends EmbedParser {
        FailingEmbedParser(final TikaDocument root, final ParseContext context) {
            super(root, context);
        }

        @Override
        void delegateParsing(final InputStream input, final ContentHandler handler, final Metadata metadata)
                throws SAXException {
            throw new SAXException(DELEGATE_FAILURE);
        }
    }

    private TikaDocument root() {
        return new DocumentFactory()
                .withIdentifier(new DigestIdentifier("SHA-256", Charset.defaultCharset()))
                .create(Paths.get("/tmp/root.zip"));
    }

    private Metadata named(final String name) {
        final Metadata metadata = new Metadata();
        metadata.set(TikaCoreProperties.RESOURCE_NAME_KEY, name);
        return metadata;
    }

    private InputStream entryBytes() {
        return new ByteArrayInputStream("entry".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void aFailedEmbedDoesNotLeakPackageEntryDepthOntoItsSibling() throws Exception {
        // Tika's real accounting, with the package-entry limit lowered so a SINGLE leaked entry is
        // enough to refuse the next sibling (startElement throws once size() >= the limit).
        final SecureContentHandler secure =
                new SecureContentHandler(new DefaultHandler(), TikaInputStream.get(new byte[0]));
        secure.setMaximumPackageEntryDepth(2);
        final EmbedParser parser = new FailingEmbedParser(root(), new ParseContext());

        // First embed fails. Propagating is correct (the caller decides whether to keep walking), but
        // it must not leave this embed's package-entry div open.
        try {
            parser.parseEmbedded(entryBytes(), secure, named("first.txt"), true);
            throw new AssertionError("expected the delegate parse to fail");
        } catch (final SAXException expected) {
            assertThat(expected.getMessage()).isEqualTo(DELEGATE_FAILURE);
        }

        // The sibling must still be emittable, so the only failure it can report is its own delegate
        // failure. RED before the fix: writeStart throws SecureSAXException "2 levels of package entry
        // nesting" instead, because the first embed's entry was never popped.
        try {
            parser.parseEmbedded(entryBytes(), secure, named("second.txt"), true);
            throw new AssertionError("expected the delegate parse to fail");
        } catch (final SAXException expected) {
            assertThat(expected.getMessage()).isEqualTo(DELEGATE_FAILURE);
        }
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `mvn -o -pl extract-lib test -Dtest=EmbedParserPackageEntryBalanceTest`

Expected: FAIL on the second `parseEmbedded`, with an assertion comparing the actual message `Suspected zip bomb: 2 levels of package entry nesting` against the expected `embed parse failed`. If it fails anywhere else, stop and re-read the test before touching production code.

- [ ] **Step 3: Make the minimal change**

In `EmbedParser.java`, replace the body of `parseEmbedded` (tabs, matching the file):

```java
	@Override
	public void parseEmbedded(final InputStream input, final ContentHandler handler, final Metadata metadata,
	                          final boolean outputHtml) throws SAXException, IOException {
		if (outputHtml) {
			writeStart(handler, metadata);
		}

		// writeEnd MUST run even when the delegate parse throws. Tika's SecureContentHandler pops a
		// package entry only on the matching endElement, and its counters live for the whole root
		// parse, so a skipped writeEnd leaves this embed's <div class="package-entry"> open forever.
		// Callers of this walk tolerate per-entry failures and continue with the next sibling, so each
		// unclosed div permanently raises the nesting count until the guard starts refusing embeds
		// that are barely nested at all. Note writeStart stays OUTSIDE the try: if it throws, its
		// startElement never reached the downstream handler, so emitting a matching endElement would
		// push an unmatched end event downstream.
		try {
			delegateParsing(input, new EmbeddedContentHandler(new BodyContentHandler(handler)), metadata);
		} finally {
			if (outputHtml) {
				writeEnd(handler);
			}
		}
	}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `mvn -o -pl extract-lib test -Dtest=EmbedParserPackageEntryBalanceTest`

Expected: PASS, 1 test.

- [ ] **Step 5: Run the neighbouring suites that exercise the failure path**

Run: `mvn -o -pl extract-lib test -Dtest='EmbedParser*Test,EmbeddedDocumentExtractor*Test,EmbedSpawner*Test'`

Expected: all PASS. These cover embed-boundary behavior most likely to notice a changed emission order on the failure path (`EmbedParserCancellationTest`, `EmbedParserMarkerTest`, `EmbeddedDocumentExtractorStackBalanceTest`, `EmbeddedDocumentExtractorUnreadableEmbedTest`). If one fails, the emission change is the suspect: check whether the test asserts on element order or on a document stack that is now unwound differently.

- [ ] **Step 6: Commit**

Use the `/commit` skill. Intended message:

```
fix(artifacts): close the package-entry div even when an embed parse fails

A SAXException from an embed skipped writeEnd, and SecureContentHandler pops a
package entry only on the matching endElement, so every failed embed raised the
nesting count for the rest of the root parse until barely-nested embeds were
refused. Run writeEnd in a finally.
```

---

### Task 2: Explicit embed-depth guard at index parity

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/extract/extractor/EmbeddedDocumentExtractor.java:179-186` (top of `DigestEmbeddedDocumentExtractor.delegateParsing`)
- Test: `extract-lib/src/test/java/org/icij/extract/extractor/EmbeddedDocumentExtractorEmbedDepthTest.java` (create)
- Delete: `extract-lib/src/test/java/org/icij/extract/extractor/EmbeddedDocumentExtractorZipBombDepthTest.java`

**Interfaces:**
- Consumes: `EmbedSpawner.DEFAULT_MAX_EMBED_DEPTH` (package-private `static final int`, value 20) and `EmbedSpawner.exceedsMaxEmbedDepth(int stackSize, int maxEmbedDepth)` (package-private `static boolean`, returns `maxEmbedDepth > 0 && stackSize > maxEmbedDepth`). Both are in the same package, so no visibility change is needed. Also consumes Task 1's guarantee that the wrapper div is closed, since a refused embed returns from inside `parseEmbedded`'s `try`.
- Produces: behavior only. No new public or package-private members.

**Why this check site:** INDEX checks depth in `EmbedSpawner.parseEmbedded` (`EmbedSpawner.java:382`) against `baseDepthOffset + tikaDocumentStack.size()`, before `tikaDocumentStack.add(embed)` at line 471, with the stack seeded with the root at line 207. So the stack size it tests equals the nesting level of the embed being considered: 1 for a top-level embed, and an embed is refused when that level exceeds 20. The artifact walk seeds `documentStack` with the root identically (`EmbeddedDocumentExtractor.java:173`) and pushes the embed at line 186, so testing `documentStack.size()` before line 181 gives the identical level for the identical embed. Placing the check before `addEmbed` also mirrors INDEX in not attaching a refused embed to its parent.

- [ ] **Step 1: Delete the superseded test**

```bash
git rm -q extract-lib/src/test/java/org/icij/extract/extractor/EmbeddedDocumentExtractorZipBombDepthTest.java
```

It is tracked as of the baseline commit, so use `git rm` and let the removal land in this task's commit. Its `nestedZip` helper is carried into the new test in Step 2.

- [ ] **Step 2: Write the failing test**

Create `extract-lib/src/test/java/org/icij/extract/extractor/EmbeddedDocumentExtractorEmbedDepthTest.java`:

```java
package org.icij.extract.extractor;

import org.icij.extract.document.DigestIdentifier;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.TikaDocument;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.fest.assertions.Assertions.assertThat;

/**
 * The ARTIFACT re-extraction walk must cache exactly what the index accepted: every embed nested no
 * deeper than EmbedSpawner.DEFAULT_MAX_EMBED_DEPTH, and nothing deeper. Tika's SecureContentHandler
 * depth counters cannot express that bound here (they count the whole root parse and are corrupted by
 * any partial embed failure), so the walk carries its own guard, reusing the index's constant and
 * predicate. These tests pin both ends of the bound, including the off-by-one at the check site.
 */
public class EmbeddedDocumentExtractorEmbedDepthTest {

    @Rule public TemporaryFolder tmp = new TemporaryFolder();

    @Test
    public void cachesEveryLevelWithinTheIndexDepthBound() throws Exception {
        final int depth = EmbedSpawner.DEFAULT_MAX_EMBED_DEPTH - 5; // 15, comfortably inside the bound

        // Every level is within the bound, so every level is cached. This also guards the reason the
        // zip-bomb depth relaxation exists: Tika's default 100-level XML nesting counter aborts this
        // walk around level 9, far below the bound the index actually applies.
        assertThat(cachedRawCount(nestedZip(depth))).isEqualTo(depth);
    }

    @Test
    public void refusesLevelsPastTheIndexDepthBound() throws Exception {
        final int bound = EmbedSpawner.DEFAULT_MAX_EMBED_DEPTH; // 20
        final int depth = bound + 5;

        // Levels 1..bound are kept, everything below is refused, so retrieval caches no more than the
        // index produced. An off-by-one at the check site shows up here as bound-1 or bound+1.
        assertThat(cachedRawCount(nestedZip(depth))).isEqualTo(bound);
    }

    // Runs the full ARTIFACT walk over `zipBytes` and counts the raw payloads it cached. The nested
    // zip yields exactly one embed per level, so the count IS the deepest level reached.
    private int cachedRawCount(final byte[] zipBytes) throws Exception {
        final Path zip = tmp.newFile().toPath();
        Files.write(zip, zipBytes);
        final Path artifacts = tmp.newFolder().toPath();

        final EmbeddedDocumentExtractor extractor =
                new EmbeddedDocumentExtractor(new UpdatableDigester("prj", "SHA-384"), artifacts);
        final TikaDocument document = new DocumentFactory()
                .withIdentifier(new DigestIdentifier("SHA-384", StandardCharsets.UTF_8))
                .create(zip);

        extractor.extractAll(document);

        try (var paths = Files.walk(artifacts)) {
            return (int) paths.filter(p -> p.getFileName().toString().equals("raw")).count();
        }
    }

    // Builds `depth` zips nested one inside the next, the innermost wrapping a small text leaf. The
    // outermost zip is the root document, so its entries are the embeds at levels 1..depth.
    private static byte[] nestedZip(final int depth) throws Exception {
        byte[] current = "needle".getBytes(StandardCharsets.UTF_8);
        String name = "leaf.txt";
        for (int level = 0; level < depth; level++) {
            final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (ZipOutputStream zip = new ZipOutputStream(bytes)) {
                zip.putNextEntry(new ZipEntry(name));
                zip.write(current);
                zip.closeEntry();
            }
            current = bytes.toByteArray();
            name = "level" + level + ".zip";
        }
        return current;
    }
}
```

- [ ] **Step 3: Run both tests and confirm each is RED for the right reason**

Run: `mvn -o -pl extract-lib test -Dtest=EmbeddedDocumentExtractorEmbedDepthTest`

Expected: `refusesLevelsPastTheIndexDepthBound` FAILS with `expected:<20> but was:<25>`, because no bound exists yet. That is this task's RED driver.

`cachesEveryLevelWithinTheIndexDepthBound` is expected to already PASS, because the uncommitted `relaxZipBombGuard` is what lets the walk past level 9. Confirm it is a real guard rather than a vacuous one by checking it against the pre-relaxation code:

```bash
F=extract-lib/src/main/java/org/icij/extract/extractor/EmbeddedDocumentExtractor.java

# unwrap both relaxZipBombGuard call sites, leaving only its definition
perl -0pi -e 's/return relaxZipBombGuard\((new AutoDetectParser\(.+?\))\);/return $1;/gs' "$F"
grep -c relaxZipBombGuard "$F"   # expect 1, the definition only

mvn -o -pl extract-lib test -Dtest='EmbeddedDocumentExtractorEmbedDepthTest#cachesEveryLevelWithinTheIndexDepthBound'
# expect FAIL: expected:<15> but was:<9>

git checkout -- "$F"             # the baseline commit is the restore point
grep -c relaxZipBombGuard "$F"   # expect 3 again: definition + 2 call sites
```

Do this check BEFORE Step 4, while the file still matches the baseline commit, so `git checkout --` restores cleanly. If you have already written the Step 4 guard, copy the file aside first and restore from that copy instead.

If either exact count differs from 15 or 25 by one, do not weaken the assertion to an inequality. Find out which level the extra or missing raw belongs to by reading the `resourceName` in the `raw.json` sidecars, then pin the true number with a comment saying why (for example whether the leaf text file counts as its own level).

- [ ] **Step 4: Add the guard**

In `EmbeddedDocumentExtractor.java`, at the very top of `DigestEmbeddedDocumentExtractor.delegateParsing`, before the existing `addEmbed` call (4-space indentation, matching the file):

```java
        @Override
        void delegateParsing(InputStream stream, ContentHandler handler, Metadata metadata) throws IOException, SAXException {
            // Nesting bound, mirroring INDEX exactly: same constant, same predicate, same check site
            // relative to the stack push (see EmbedSpawner.parseEmbedded, which tests
            // baseDepthOffset + tikaDocumentStack.size() before pushing the embed). documentStack is
            // seeded with the root, so its size here IS the nesting level of the embed being
            // considered. Checking before addEmbed also matches INDEX in not attaching a refused
            // embed to its parent. This is the walk's real depth protection: Tika's
            // SecureContentHandler counters cannot serve as the bound here because they accumulate
            // across the whole root parse and any embed that fails mid-document leaves them inflated
            // (see relaxZipBombGuard).
            if (EmbedSpawner.exceedsMaxEmbedDepth(this.documentStack.size(), EmbedSpawner.DEFAULT_MAX_EMBED_DEPTH)) {
                logger.warn("Embedded document nested deeper than {} levels not extracted: \"{}\" (in \"{}\").",
                        EmbedSpawner.DEFAULT_MAX_EMBED_DEPTH, metadata.get(TikaCoreProperties.RESOURCE_NAME_KEY),
                        this.documentStack.getFirst());
                return;
            }
            EmbeddedTikaDocument embed = this.documentStack.getLast().addEmbed(metadata);
```

Leave the rest of the method exactly as it is. Notes for the implementer:

- `logger` is the enclosing class's `private static final Logger` (`EmbeddedDocumentExtractor.java:41`); a nested class reaches it directly. `TikaCoreProperties` is already imported (line 16).
- The WARN mirrors `EmbedParser.delegateParsing`'s single-line style. Unlike INDEX, do not record a metadata counter: the artifact walk's metadata is not indexed, so a counter there would have no reader.
- A plain `return` is deliberate. The embed is skipped, no bytes are written, the walk continues with the next sibling, and the root document does not fail.

- [ ] **Step 5: Run the test to verify both pass**

Run: `mvn -o -pl extract-lib test -Dtest=EmbeddedDocumentExtractorEmbedDepthTest`

Expected: PASS, 2 tests.

- [ ] **Step 6: Run the extractor and spawner suites**

Run: `mvn -o -pl extract-lib test -Dtest='EmbeddedDocumentExtractor*Test,Extractor*Test,EmbedSpawner*Test'`

Expected: all PASS. The guard changes which embeds are reached, so a fixture nested deeper than 20 would surface here. If one fails, check the fixture's nesting depth before assuming the guard is wrong: refusing past 20 is the intended behavior, and a fixture that legitimately nests deeper needs the test updated, not the bound raised.

- [ ] **Step 7: Commit**

Use the `/commit` skill. Intended message:

```
fix(artifacts): bound the retrieval walk's embed nesting at index parity
```

Subject line only. This repo's convention is a single-line semantic message with no body and no trailers.

---

### Task 3: Restate relaxZipBombGuard and drop the inline FQNs

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/extract/extractor/EmbeddedDocumentExtractor.java` (imports near line 11, `relaxZipBombGuard` comment and body near line 437, `discardingHandler` body near line 96)

**Interfaces:**
- Consumes: Task 2's guard, which this comment now points at as the real bound.
- Produces: no behavior change. `relaxZipBombGuard(AutoDetectParser)` keeps its signature and its two call sites.

This task is comment and import hygiene only. There is no test step, because there is no behavior change to test; the suites run in Task 5 confirm nothing moved.

- [ ] **Step 1: Add the two imports**

In the import block, add `AutoDetectParserConfig` next to the existing `AutoDetectParser` import (line 11). `java.io.Writer` needs no new import: the file already imports `java.io.*` (line 30).

```java
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.AutoDetectParserConfig;
```

- [ ] **Step 2: Replace the inline fully qualified names**

In `discardingHandler`:

```java
        return new BodyContentHandler(Writer.nullWriter());
```

In `relaxZipBombGuard`, the local declaration becomes:

```java
        AutoDetectParserConfig config = new AutoDetectParserConfig();
```

- [ ] **Step 3: Restate the relaxZipBombGuard comment**

Replace the whole comment above `relaxZipBombGuard` with one that names the current bound, so a future reader does not restore the counters believing they are the protection:

```java
    // Turns OFF Tika's SecureContentHandler DEPTH counters for this walk. They cannot serve as its
    // bound: they accumulate across the whole root parse, they are inflated by any embed that throws
    // mid-document (endElement, and therefore the package-entry pop, never runs for the elements that
    // parser left open), and SecureSAXException is a SAXException rather than a TikaException, so
    // EmbedParser's tolerant catch misses it and the embed is dropped from the cache even though the
    // index kept it. Left at their defaults, one early failure therefore cascades: each failure raises
    // the count until embeds that are barely nested are refused, losing most of a large container.
    // The real nesting bound lives in DigestEmbeddedDocumentExtractor.delegateParsing, which refuses
    // embeds nested deeper than EmbedSpawner.DEFAULT_MAX_EMBED_DEPTH exactly as the index does. The
    // COMPRESSION-RATIO guard, which is what actually detects a real bomb, keeps its default here.
```

- [ ] **Step 4: Verify it still compiles and the depth tests still pass**

Run: `mvn -o -pl extract-lib test -Dtest='EmbeddedDocumentExtractorEmbedDepthTest,EmbedParserPackageEntryBalanceTest'`

Expected: PASS, 3 tests.

- [ ] **Step 5: Commit**

Use the `/commit` skill. Intended message:

```
docs(artifacts): say where the retrieval walk's depth bound actually lives
```

Subject line only. This repo's convention is a single-line semantic message with no body and no trailers.

---

### Task 4: Cover the discarding handler swap

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/extract/extractor/EmbeddedDocumentExtractor.java:95` (`discardingHandler` visibility)
- Test: `extract-lib/src/test/java/org/icij/extract/extractor/EmbeddedDocumentExtractorDiscardingHandlerTest.java` (create)

**Interfaces:**
- Consumes: nothing from other tasks.
- Produces: `static ContentHandler discardingHandler()` becomes package-private (drops `private`). Signature and behavior are otherwise unchanged, and its two callers (`extractAll`, `extract`) are untouched.

**What this test can and cannot prove:** the swap from `BodyContentHandler(-1)` to a null writer exists because the unbounded buffer overflows Java's array limit on a very large body and aborts the whole root document with an `OutOfMemoryError` that no `Exception | LinkageError` handler catches. That scale is not reachable in CI. The test pins the property whose loss caused it, so a future refactor back to a buffering handler fails a test instead of failing in production.

Two API facts the test depends on, both checked against Tika 3.3.0. `BodyContentHandler` only forwards events matching `/xhtml:html/xhtml:body/descendant::node()`, so characters fed outside a body element are dropped and the test would pass vacuously; feeding through `XHTMLContentHandler` emits the required skeleton. And `ToTextContentHandler.toString()` returns `writer.toString()`, so with `Writer.nullWriter()` it returns that writer's identity string rather than an empty string; assert the text is absent, not that the result is empty.

- [ ] **Step 1: Write the failing test**

Create `extract-lib/src/test/java/org/icij/extract/extractor/EmbeddedDocumentExtractorDiscardingHandlerTest.java`:

```java
package org.icij.extract.extractor;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.sax.XHTMLContentHandler;
import org.junit.Test;
import org.xml.sax.ContentHandler;

import static org.fest.assertions.Assertions.assertThat;

/**
 * The ARTIFACT/download walk needs each embed's bytes and digest, never the extracted text: bytes are
 * written by documentCallback via Files.copy. A buffering handler therefore accumulates a whole
 * document's text for nothing, and on a very large body it overflows Java's array limit and aborts the
 * entire root document with an OutOfMemoryError that the per-embed Exception | LinkageError handlers
 * cannot catch. That scale is not reproducible in CI, so this pins the property instead: the handler
 * the walk parses into must not retain body text.
 */
public class EmbeddedDocumentExtractorDiscardingHandlerTest {

    private static final String BODY_TEXT = "text the artifact walk never reads";

    @Test
    public void doesNotAccumulateBodyText() throws Exception {
        final ContentHandler handler = EmbeddedDocumentExtractor.discardingHandler();

        // BodyContentHandler forwards only /xhtml:html/xhtml:body/descendant::node(), so the text has
        // to arrive inside a real XHTML skeleton or this test would pass without proving anything.
        final XHTMLContentHandler xhtml = new XHTMLContentHandler(handler, new Metadata());
        xhtml.startDocument();
        xhtml.characters(BODY_TEXT);
        xhtml.endDocument();

        // RED with BodyContentHandler(-1), whose unbounded StringWriter hands the text back here.
        assertThat(handler.toString().contains(BODY_TEXT)).isFalse();
    }
}
```

- [ ] **Step 2: Make discardingHandler reachable from the test**

In `EmbeddedDocumentExtractor.java`, drop `private` so the same-package test can call it:

```java
    static ContentHandler discardingHandler() {
```

Add one line to the end of its existing comment so the widened visibility is not mistaken for an oversight:

```java
    // Package-private rather than private so EmbeddedDocumentExtractorDiscardingHandlerTest can pin
    // the no-buffering property directly; it has no other callers outside this class.
```

- [ ] **Step 3: Run the test to verify it passes**

Run: `mvn -o -pl extract-lib test -Dtest=EmbeddedDocumentExtractorDiscardingHandlerTest`

Expected: PASS, 1 test.

- [ ] **Step 4: Verify the test is a real guard, not a vacuous one**

Temporarily change `discardingHandler`'s body back to the buffering form, run the test, and confirm it fails. Then restore the null writer.

```bash
# in EmbeddedDocumentExtractor.java, temporarily:  return new BodyContentHandler(-1);
mvn -o -pl extract-lib test -Dtest=EmbeddedDocumentExtractorDiscardingHandlerTest
# expect FAIL: expected:<false> but was:<true>
# then restore:  return new BodyContentHandler(Writer.nullWriter());
```

If it passes with `BodyContentHandler(-1)`, the text is not reaching the handler at all: check that `startDocument` runs before `characters` and that the assertion reads `handler.toString()` rather than the `xhtml` wrapper's.

- [ ] **Step 5: Commit**

Use the `/commit` skill. Intended message:

```
test(artifacts): pin that the retrieval walk discards body text
```

Subject line only. This repo's convention is a single-line semantic message with no body and no trailers.

---

### Task 5: Full-module verification

**Files:** none modified.

**Interfaces:**
- Consumes: Tasks 1 through 4.
- Produces: the evidence that the four changes hold together.

- [ ] **Step 1: Run the whole extract-lib suite**

Run: `mvn -o -pl extract-lib test`

Expected: BUILD SUCCESS, zero failures and zero errors. Note that per the Makefile, `make test` expects a running Redis; if a Redis-dependent test fails, confirm it also fails on a clean checkout before treating it as a regression from this work.

- [ ] **Step 2: Confirm the four changes are all present**

```bash
grep -c relaxZipBombGuard extract-lib/src/main/java/org/icij/extract/extractor/EmbeddedDocumentExtractor.java   # 3: definition + 2 call sites
grep -c 'Writer.nullWriter()' extract-lib/src/main/java/org/icij/extract/extractor/EmbeddedDocumentExtractor.java # 1
grep -c exceedsMaxEmbedDepth extract-lib/src/main/java/org/icij/extract/extractor/EmbeddedDocumentExtractor.java  # 1
grep -n 'finally' extract-lib/src/main/java/org/icij/extract/extractor/EmbedParser.java                           # the writeEnd finally
```

- [ ] **Step 3: Confirm no identifiers leaked into the diff**

```bash
git diff | grep -inE '<confidential identifiers>' && echo "LEAK: remove before shipping" || echo "clean"
```

Expected: `clean`. Comments must describe mechanisms only.

- [ ] **Step 4: Report**

Summarize for review: the four commits, the RED evidence observed for each test (including the exact failure messages from Task 1 Step 2, Task 2 Step 3, and Task 4 Step 4), and the full-suite result. State plainly that the >2GB OOM itself is not covered by any test and why.

## Self-Review

**Spec coverage:** Unit 1 is Task 1. Unit 2 is Task 2, including the parity check site and the rewritten test. Unit 3 is Task 3. Unit 4 is Task 4. The spec's Style section is Task 3 Steps 1 and 2. The spec's Verification section is Task 5 plus the per-task suite runs. The spec's rewritten-test requirement is Task 2 Steps 1 and 2. Nothing in the spec is unassigned.

**Placeholders:** none. Every code step carries the code, every run step carries the command and the expected result.

**Type consistency:** `exceedsMaxEmbedDepth(int, int)` and `DEFAULT_MAX_EMBED_DEPTH` are used with the signatures verified in `EmbedSpawner.java:55` and `:874`. `discardingHandler()` is `static ContentHandler` in both the production change (Task 4 Step 2) and the test call (Task 4 Step 1). `delegateParsing(InputStream, ContentHandler, Metadata)` matches the override in `EmbeddedDocumentExtractor.java:180` and the test override in Task 1. `SecureContentHandler(ContentHandler, TikaInputStream)` and `setMaximumPackageEntryDepth(int)` match tika-core 3.3.0.

**Known soft spot:** the two exact counts in Task 2 (15 and 20) are derived from the nested-zip structure rather than measured. Task 2 Step 3 tells the implementer to pin the observed value with a documented reason instead of loosening the assertion to an inequality, since the exact count is what catches an off-by-one at the check site.
