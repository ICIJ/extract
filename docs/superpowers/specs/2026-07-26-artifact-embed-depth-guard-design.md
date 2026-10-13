# Bound the ARTIFACT embed walk explicitly instead of relying on Tika's SAX depth counters

Date: 2026-07-26

## Goal

Make the ARTIFACT / download re-extraction walk (`EmbeddedDocumentExtractor.extractAll` / `extract`, built on `EmbedParser`) reach every embedded document the INDEX pass accepted, refuse exactly what INDEX refuses, and keep a hard bound on nesting for hostile input. Replace the current reliance on Tika's `SecureContentHandler` depth counters, which cannot be trusted on a path that tolerates per-embed parse failures.

Two caveats to "refuse exactly what INDEX refuses": the guard reuses `EmbedSpawner.DEFAULT_MAX_EMBED_DEPTH` directly, not the effective `maxEmbedDepth` (an `Extractor` CLI option that overrides the default at index time, see `Extractor.java:150,319-324`), so a deployment that configures `maxEmbedDepth` makes retrieval refuse at a different level than INDEX did -- do not "fix" this by changing the index option; the real fix is to pass the effective depth into the guard. Separately, INDEX does not push a stack entry for INLINE embeds (`EmbedSpawner.java:359-374` handles them in a branch with no depth check and no push), while this walk pushes for every embed, so near the bound retrieval can run one level ahead of INDEX on inline-heavy documents.

## Why

`EmbedParser` wraps each emitted embed in a nested `<div class="package-entry">` (`EmbedParser.writeStart` / `writeEnd`). Tika's `SecureContentHandler` counts XML element nesting (`maximumDepth`, default 100) and package-entry nesting (`maximumPackageEntryDepth`, default 10) across the whole root parse, and throws `SecureSAXException` past either limit.

Three facts about that interaction, all verified against Tika 3.3.0 sources and this repo:

1. `SecureSAXException` extends `SAXException`, not `TikaException`, so `EmbedParser.delegateParsing`'s tolerant `catch (TikaException)` (whose comment explicitly covers "nested zip bomb") does not catch it. The embed is dropped from the artifact cache even though INDEX kept it, because INDEX routes content through `EmbedSpawner`'s spew sink and never accumulates the nesting.
2. `writeEnd` is not in a `finally`, so any `SAXException` from an embed's parse skips it and leaves the wrapper div open. Callers that tolerate per-entry failures continue the walk (for example `ResilientOutlookPSTParser.java:471-480`, which swallows `Exception | LinkageError` per message), so `SecureContentHandler.currentDepth` stays permanently elevated and one entry stays stuck in `packageEntryDepths`. Each embed level adds exactly one `<div class="package-entry">`, and `packageEntryDepths`' default limit is 10, not 100, so it is the package-entry counter, not the element counter, that gets exhausted first: past roughly nine failures every remaining embed is refused regardless of its real nesting, so a single early failure cascades into losing most of a large container's entries.
3. Balancing our own div is not sufficient to unwind that leak. `SecureContentHandler.endElement` pops a package entry only when `packageEntryDepths.getLast() == currentDepth`. When the delegate parser throws mid-document its own open elements are never closed either, so `currentDepth` is already inflated when `writeEnd` fires, the pop condition fails, and the entry leaks anyway. Making Tika's counters reliable here would require a SAX-stack-unwinding decorator around every embed parse.

Conclusion: Tika's depth counters are the wrong bound for this path. The artifact walk needs its own explicit nesting bound, which is also the only way to reach parity with INDEX, since INDEX bounds embed nesting at `EmbedSpawner.DEFAULT_MAX_EMBED_DEPTH` (20) rather than at the package-entry counter's 10.

The current uncommitted state of `EmbeddedDocumentExtractor` relaxes both Tika depth counters to `Integer.MAX_VALUE`. That removes the cascade but leaves the walk with no nesting bound at all, so it can descend deeper and cache more than INDEX ever produced. This design keeps the relaxation and supplies the real bound.

## Design

### Unit 1: balanced package-entry emission

`EmbedParser.parseEmbedded` puts `writeEnd` in a `finally` so a `SAXException` from an embed cannot leave its wrapper div open.

```java
if (outputHtml) {
    writeStart(handler, metadata);
}
try {
    delegateParsing(input, new EmbeddedContentHandler(new BodyContentHandler(handler)), metadata);
} finally {
    if (outputHtml) {
        writeEnd(handler);
    }
}
```

`writeStart` stays outside the `try`: if it throws, its `startElement` never reached the downstream handler, so emitting a matching `endElement` would push an unmatched end downstream.

This does not by itself restore Tika's counters (see Why, point 3). Its value is that the emitted XHTML stays well formed for any consumer of this walk, and `packageEntryDepths` stops growing by one entry per emitted embed that fails.

Test: two sibling embeds where the first's delegate parse throws a `SAXException`. Assert the recorded start and end element counts balance, and that the second embed is still emitted. RED before the change (one unclosed div), GREEN after.

### Unit 2: explicit embed-depth guard at index parity

The artifact walk gets its own nesting bound, mirroring the constant INDEX uses (`EmbedSpawner.DEFAULT_MAX_EMBED_DEPTH`, 20). An embed nested deeper than the bound is refused, deterministically, independent of any SAX counter state.

No new depth tracking is needed. `DigestEmbeddedDocumentExtractor` already keeps a balanced `documentStack` (`EmbeddedDocumentExtractor.java:162`, root added at 173, embed pushed at 186, popped in a `finally` at 313), and the depth predicate already exists as a static helper: `EmbedSpawner.exceedsMaxEmbedDepth(stackSize, maxEmbedDepth)`.

The guard is therefore one call in `DigestEmbeddedDocumentExtractor.parseEmbedded`, before the embed is pushed and before any spool or recursion:

```java
if (EmbedSpawner.exceedsMaxEmbedDepth(documentStack.size(), EmbedSpawner.DEFAULT_MAX_EMBED_DEPTH)) {
    // refused, WARN, return
}
```

Reusing the same helper and the same constant is what makes parity real rather than coincidental. The check site must sit at the same point relative to the stack push that `EmbedSpawner.java:382` uses, so the two paths refuse at the identical nesting level rather than one level apart. That off-by-one is pinned by a test, not by reading, because it is the whole point of the unit.

A refused embed is skipped with a single WARN, matching how `delegateParsing` reports a per-entry data quirk. It is not an error for the root document.

Tests:

- Containers nested about 15 levels deep are all cached. RED against the pre-change code, where the 10-level package-entry counter aborts the walk around level 9 (one `<div class="package-entry">` per embed level, the 10th throws).
- Containers nested past 20 levels are refused, so the artifact walk caches no more than INDEX would.
- Parity: for the same nested input, the deepest embed the artifact walk caches is the deepest one `EmbedSpawner` emits at the same `maxEmbedDepth`. This is the test that pins the check site, and it fails if the guard is one level off in either direction.

### Unit 3: keep `relaxZipBombGuard`, restated

`relaxZipBombGuard` stays as written, relaxing `maximumDepth` and `maximumPackageEntryDepth` on both artifact parser builders (`createParserWithoutOCR`, `withResilientPstParser`). With Unit 2 supplying the bound, those counters are no longer load bearing, and their state is corrupted by any partial embed failure, so leaving them relaxed is correct rather than merely convenient.

The compression-ratio guard (`maximumCompressionRatio`) keeps its default, and nothing in this design touches it. Note what it does and does not cover: `SecureContentHandler.advance` compares output characters against root input bytes, so it catches text-expansion bombs, while a pure nested-archive bomb emits almost no characters and effectively never trips it. Archive-nesting depth is bounded by Unit 2's guard instead.

The method's comment is rewritten to say the nesting bound now lives in Unit 2, so a future reader does not restore the counters believing they are the protection.

No settings are lost by replacing the config: `AutoDetectParserConfig.DEFAULT` is itself `new AutoDetectParserConfig()`, all fields null-defaulted, and an `AutoDetectParser` built without a `TikaConfig` uses that default.

### Unit 4: cover the discarding handler swap

`extractAll` and `extract` now build their handler with `discardingHandler()` (`BodyContentHandler(Writer.nullWriter())`) instead of `BodyContentHandler(-1)`, because the artifact path never reads the extracted text (embed bytes are written by `documentCallback` via `Files.copy`) and an unbounded buffer overflows Java's array limit on a very large body, aborting the whole root document with an `OutOfMemoryError` that no `Exception | LinkageError` handler catches.

That swap currently has no test at any level. `discardingHandler()` becomes package-private and gets one: feed characters through the returned handler, assert nothing accumulates. RED with `BodyContentHandler(-1)`, which buffers and returns the text, GREEN with the null writer.

This does not prove the absence of an OOM at multi-gigabyte scale, which is not unit-testable in CI. It locks the property whose loss caused the OOM, so a future refactor back to a buffering handler fails a test instead of failing in production.

### Rewritten test

`EmbeddedDocumentExtractorZipBombDepthTest` currently asserts that a 130-level nested zip yields at least 130 raw artifacts. That contradicts Unit 2's 20-level bound, so it is replaced by the two Unit 2 tests. The nested-zip builder in it is reused.

## Style

Add imports for `java.io.Writer` and `org.apache.tika.parser.AutoDetectParserConfig` rather than using inline fully qualified names, matching the rest of the file.

## Out of scope

- A SAX-stack-unwinding decorator that would make Tika's depth counters reliable per embed. Unit 2 removes the need for it.
- The ARTIFACT stage gating root success on an empty manifest while ignoring the `complete:false` it records, so a silent under-cache reads as success. That is a datashare-side hardening, tracked separately.
- Any change to INDEX-time parsing. `EmbedSpawner` already bounds embed depth and is untouched here.

## Verification

TDD throughout: each unit's test is written first and observed RED against current `master` plus the uncommitted changes, then GREEN.

Full `mvn -pl extract-lib test` must pass, with attention to the existing extractor suite (`EmbeddedDocumentExtractorStackBalanceTest`, `EmbeddedDocumentExtractorPstIdParityTest`, `EmbeddedDocumentExtractorUnreadableEmbedTest`, `ExtractorTest`), since Unit 1 changes emission order on the failure path and Unit 2 changes which embeds are reached.
