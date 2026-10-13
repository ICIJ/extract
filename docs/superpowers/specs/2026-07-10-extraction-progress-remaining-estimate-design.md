# Extraction progress: estimated "what's left" for big containers

Date: 2026-07-10
Status: Design approved, pending implementation plan
Repo: extract (extract-lib), with a trivial follow-up version bump in datashare

## Problem

The per-file progress heartbeat reports only work done, never work remaining:

```
/data/mailboxes/foo.ost: 44280s, 233140 embeds, 2582/2582 OCR done, 0 skipped(maxDepth), 0 skipped(maxSize)
```

For a giant OST or archive that parses for hours, an operator cannot tell "still making progress, 70% in" from "wedged". The `embeds` counter has no denominator. The OCR segment (`2582/2582`) already carries a ratio; the embed segment does not.

The root document of a container is only indexed at the end of its parse (the streaming spewer reads the root's content last, which is what drives the whole parse), so during those hours the only live signal is this heartbeat line. Making it show remaining work is the smallest useful improvement; it does not change indexing behavior.

## Goal

Add an estimated completion signal to the embed segment of the heartbeat line for containers whose total sub-unit count is cheaply knowable up front:

```
foo.ost: 44280s, 233140/~310000 embeds (~75%), 2582/2582 OCR done, 0 skipped(maxDepth), 0 skipped(maxSize)
big.zip: 900s, 41200/~58000 embeds (~71%), 0/0 OCR done, ...
```

Containers without a cheap total keep today's count-only line unchanged.

## Non-goals

- Throughput / rate reporting. Explicitly declined: the ask is remaining work, not speed.
- An exact remaining count. The total is an estimate, signalled by the `~` prefix.
- Progress for arbitrary container types. Only PST/OST and ZIP/7z have a cheap ground-truth total; everything else falls back.
- Any change to what gets indexed, when the root is written, or the streaming/abort logic.

## Key insight: the percentage is real, the embed total is estimated

For both container families the shape is identical: there is a "unit" whose total is cheaply knowable (PST message, archive entry), a running embed count that includes deeper nesting (attachments, nested archives), and we want to express progress in the embed unit the operator watches.

Estimate the embed total by scaling the unit total by the observed embeds-per-unit ratio:

```
estTotal = embedsParsed * (expectedUnits / unitsParsed)
```

The displayed percentage `embedsParsed / estTotal` then algebraically reduces to:

```
pct = unitsParsed / expectedUnits
```

So the percentage is driven by ground-truth unit progress, while the big number rendered to the operator stays in embeds. The `~` on the total communicates that the embed denominator is a projection.

### Accuracy: best-effort, not a guarantee (revised after whole-branch review)

The percentage is a best-effort completion estimate, not an exact fraction of work. The numerator (units counted during parsing) and the denominator (the pre-counted / enumerated total) come from independent mechanisms that can disagree, so the design accepts and bounds that:

- **PST/OST reaches exactly 100% on completion.** The numerator tracks emitted messages live during the walk (real progress), then `completeUnits()` snaps it to the enumerated total when the owning parse finishes. Message loss (`expected 7, emitted 6`) therefore no longer leaves it stuck below 100%; recovery loss is still reported separately via the PST reconciliation counters.
- **Archives reach 100% for the common cases.** A top-level entry refused by the max-embed-size guard is still counted toward the numerator (it was in the denominator), so a size-guarded / zip-bomb entry no longer strands the percentage below 100%.
- **Known residual imprecision (accepted):**
  - A top-level archive entry that is itself a container (a nested ZIP/PST) is counted the moment it is entered, before its own subtree is extracted. An archive dominated by one large nested container can therefore read a high percentage while sub-extraction continues. This is inherent to using top-level entry count as the denominator; the `~` reflects it.
  - Entries a `DocumentSelector` or Tika filters before `parseEmbedded` are in the denominator but never reach the numerator, so an archive extracted under a selective configuration can plateau below 100%. Normal indexing parses all entries and is unaffected.

The display always clamps: `pct = min(100, unitsParsed*100/expectedUnits)` and `estTotal = max(embeds, round(...))`, so the line can never show >100% or a total below the running embed count, whatever the numerator/denominator skew.

## Design

### 1. Data model: `ExtractionProgress`

Add one generic pair plus an ownership flag. All counters stay `AtomicLong` (parallel PST fan-out increments concurrently).

- `expectedUnits` (`AtomicLong`, init `-1` = unknown)
  - `setExpectedUnits(long)`: set-once. A second call is ignored, so a nested container (PST-in-PST, zip-in-zip) cannot clobber the root file's total.
  - `expectedUnits()` getter.
- `unitsParsed` (`AtomicLong`, init `0`)
  - `incrementUnits()`
  - `unitsParsed()` getter.
- `parserTracksUnits` (boolean flag, default false)
  - `markParserTracksUnits()` / `parserTracksUnits()`.

A "unit" is the container's cheaply-countable top-level element: one PST message, or one archive entry. Units exist only to compute the percentage; the number shown to the operator remains the embed count.

### 2. Who fills the numbers (one numerator source per file)

| Container | Total (`expectedUnits`) | Numerator (`unitsParsed`) |
|---|---|---|
| PST/OST | PST parser: `messageDescriptorIds.size()`, set right after descriptor enumeration; only when measurable (not the unmeasurable sentinel) and only at top level. Also calls `markParserTracksUnits()`. | PST parser: `incrementUnits()` next to the existing `emission.incrementEmitted()`. |
| ZIP/7z | `Extractor` pre-count before the Tika parse: `ZipFile` / `SevenZFile` entry count, excluding directory entries. Leaves `-1` on any failure. Does NOT mark `parserTracksUnits`. | `EmbedSpawner`: on a depth-1 embed, `incrementUnits()` guarded by `!parserTracksUnits`. |
| everything else | never set (`-1`) | n/a |

The `parserTracksUnits` flag is the disambiguator: PST owns both ends and blocks `EmbedSpawner` from double-counting its messages; archives own only the total and let `EmbedSpawner` supply the numerator. Any future custom parser that knows its own unit count owns both ends by marking the flag; anything Tika parses generically relies on `EmbedSpawner` depth-1 counting plus an externally supplied total.

### 3. Wiring

**PST (ParseContext channel):** `Extractor` already builds a per-file `ParseContext` and holds the per-file `ExtractionProgress` (from `progressTracker.get(path)`) at the point it installs the `EmbedSpawner`. Add `context.set(ExtractionProgress.class, currentProgress)` there (only when non-null; it is null for page extraction and some tests). `ResilientOutlookPSTParser` reads it back from the `ParseContext`, and:
- after `enumerateMessageDescriptorIds(...)`, if the count is measurable and this is the top-level PST (not a nested/suppressed parse), calls `setExpectedUnits(size)` and `markParserTracksUnits()`;
- at each successful message emission (alongside `emission.incrementEmitted()`), calls `incrementUnits()`.
All calls are null-safe (no progress in context => no-op).

**Archive (Extractor pre-count):** before handing the `TikaInputStream` to Tika in the extraction path, the `Extractor` detects the media type. For ZIP and 7z it opens the backing file and counts entries cheaply (central directory / header metadata, no decompression), excluding directory entries, and calls `setExpectedUnits(count)` on the per-file progress. Encapsulated in a small helper (e.g. `ArchiveEntryCounter`). Any failure (corrupt, encrypted, missing backing path, unknown type) is swallowed and leaves `expectedUnits == -1`. `EmbedSpawner` supplies the numerator by calling `incrementUnits()` when it emits a depth-1 embed and `parserTracksUnits` is false.

### 4. Display: `LoggingProgressListener.formatLine`

When `expectedUnits > 0 && unitsParsed > 0`:

```
estTotal = max(embedsParsed, round(embedsParsed * expectedUnits / unitsParsed))
pct      = min(100, unitsParsed * 100 / expectedUnits)
segment  = String.format("%d/~%d embeds (~%d%%)", embedsParsed, estTotal, pct)
```

Otherwise the segment is the current `String.format("%d embeds", embedsParsed)`. The OCR segment, skip segments, elapsed seconds, and path are unchanged. The `max(embedsParsed, ...)` guard keeps the numerator from ever exceeding the shown total if the ratio drifts.

### 5. datashare

No logic change. datashare only logs what extract emits through the heartbeat. Once the extract change is released, bump `extract.version` in `datashare` `pom.xml`.

## Edge cases

- Non-PST / non-ZIP-7z container: `expectedUnits` stays `-1`, count-only line (behavior unchanged).
- Enumeration or pre-count fails (corrupt, encrypted, unknown type, no backing file): `-1`, fallback. Archive pre-count failures are swallowed and never break extraction.
- Very early in the parse (`unitsParsed == 0`): fallback to count-only until the first unit lands, avoiding divide-by-zero.
- Nested containers (PST-in-PST, zip-in-zip): only the root file's total is set (set-once plus the parser's top-level guard). Nested contents inflate `embedsParsed`, absorbed by the `~` estimate exactly like PST attachments.
- Parallel PST folder fan-out: `unitsParsed` and `embedsParsed` are atomic; concurrent increments are safe. `expectedUnits` is set once before fan-out.
- ZIP directory entries: excluded from the pre-count so the total matches the depth-1 embeds Tika actually emits (see verification checkpoint).

## Verification checkpoint (implementation-time)

The archive numerator assumes one counted ZIP/7z entry equals one depth-1 embed emitted by Tika's `PackageParser` after excluding directory entries. Assert this against a real fixture during implementation rather than trusting it. If Tika emits directory entries or coalesces entries, adjust the pre-count filter to match the emitted set. Low risk, but it is the single assumption the archive percentage rests on.

Note that PST deliberately does NOT rely on this depth-1 assumption: it sources its numerator from the parser's own message emission, so the PST percentage is independent of how deep a message sits in the embed tree. This is exactly why PST and archives use different numerator sources, and why the archive path carries the only depth-based assumption to verify.

## Affected files

extract-lib:
- `org/icij/extract/extractor/ExtractionProgress.java` (new fields, accessors)
- `org/icij/extract/extractor/LoggingProgressListener.java` (format)
- `org/icij/extract/parser/ResilientOutlookPSTParser.java` (publish total + numerator via ParseContext)
- `org/icij/extract/extractor/Extractor.java` (ParseContext wiring + archive pre-count call)
- `org/icij/extract/extractor/EmbedSpawner.java` (guarded depth-1 `incrementUnits`)
- new `ArchiveEntryCounter` helper (ZIP/7z cheap entry count)

Tests (TDD):
- `ExtractionProgressTest` (set-once, increments, flag)
- `LoggingProgressListenerTest` (existing; add estimate formatting, fallback, clamps, early-parse)
- `ResilientOutlookPSTParser` test against a PST fixture (total set, numerator increments, unmeasurable => fallback, nested PST does not clobber)
- `ArchiveEntryCounter` test (ZIP and 7z counts, directory-entry exclusion, corrupt/encrypted => no total)
- `EmbedSpawner` test (depth-1 increments only when `!parserTracksUnits`)

datashare (separate, after release):
- `pom.xml` `extract.version` bump.

## Rollout

1. Land the extract-lib change (TDD, one focused PR).
2. Cut an extract release.
3. Bump `extract.version` in datashare.

No feature flag: the estimate is derived from data the parse already produces and the archive pre-count is cheap for the supported formats. Fallback is automatic wherever a total is unavailable.
