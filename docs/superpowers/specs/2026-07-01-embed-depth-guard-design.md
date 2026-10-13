# Design: max embed-depth guard

Date: 2026-07-01
Status: approved for planning
Repo: extract (`extract-lib`), default branch `master`

## Problem

On a production OST box, indexing "never finishes" and fills the 44 GB root disk. A live thread dump showed this is not a deadlock: an "Apache Tika: <file>.zip" thread was RUNNABLE, burning hours of CPU, with a stack 33+ frames deep repeating:

```
EmbedSpawner.parseEmbedded -> PackageParser.parse -> _parse -> parseEntries
  -> parseEntry -> EmbedSpawner.spawnEmbedded -> EmbedParser.delegateParsing
  -> PackageParser.parse -> ... (repeat) ... -> Inflater.inflateBytesBytes
```

Tika's `PackageParser` recursively descends zip-inside-zip many levels deep, and each level spools a ~4 GiB entry (a literal 4 GiB member, 4294967297 bytes) to `java.io.tmpdir` via `TikaInputStream.getPath()`. With `--parallelism 9`, several 4 GiB spools coexist, `/tmp` fills, the disk fills, and the run wedges. The offending archive was a nested-zip bomb (a zip containing a 4 GiB `.bin` plus nested zips).

This is a Tika-level runaway, unrelated to the PST folder fan-out / byte-identity work.

## Decisions

Settled during brainstorming (2026-07-01):

1. **Guard dimension: maximum nesting depth only.** No per-entry byte cap, no total-extracted-bytes budget, no embed-count cap, no compression-ratio guard.
2. **Boundary behaviour: refuse.** At the depth limit an embed is not spooled, not added, not recursed into.
3. **Visibility: stub + counter.** A refused embed leaves a cheap breadcrumb: an aggregate marker on the parent document's metadata, plus a per-file progress counter. No silent data loss.
4. **Default `maxEmbedDepth = 20`, on by default, `0` disables.**
5. **Metadata marker uses the neutral `X-EXTRACT:` namespace** (no org-specific naming).

### Why depth-only, and its known limits

- The observed pathology is *runaway recursion* (33 frames and climbing). A depth cap is the direct antidote and the simplest thing to reason about: it converts an unbounded descent into a bounded one and stops the "hours of CPU / never finishes" behaviour.
- A flat per-entry byte cap was rejected because it cannot distinguish a bomb entry from a legitimately large nested file (e.g. big video footage inside a zip). The real discriminator is compression ratio, which is impractical at this enforcement point: by the time `parseEmbedded` receives the stream, `PackageParser` has already inflated the entry, so only decompressed bytes are visible, not the compressed size. (This is also why Tika's built-in `SecureContentHandler` ratio guard is useless against this bomb: it counts characters pushed to the `ContentHandler`, and a 4 GiB binary blob produces almost no handler text.)
- **Known residual (accepted):** depth bounds disk only indirectly. It caps the number of coexisting spools along a DFS path (roughly `maxDepth x largest-entry x parallelism`) and stops the descent, but it does *not* stop a shallow-but-huge single entry (e.g. one 40 GiB entry at depth 2). For the bomb observed in production (deep nest) depth is a real fix; for a hypothetical shallow bomb it is not. The existing `parseTimeout` (default 24h, configurable) is a second backstop against true hangs. Operators who want tighter disk bounds can lower `maxEmbedDepth` and/or size `java.io.tmpdir` for `budget x parallelism`.

## Enforcement

**Location:** `EmbedSpawner.parseEmbedded`, on the non-inline (spawn) branch only. Inline embeds (e.g. images concatenated into a PDF's text) do not push `tikaDocumentStack`, so they do not contribute to nesting depth and are not gated. The guard runs *before* `spawnEmbedded` / `spawnEmbeddedDeferred`, so a refused embed is never spooled to `/tmp` and never recursed into.

**Depth semantics:** `tikaDocumentStack` is seeded with `[root]` (size 1). `spawnEmbedded` pushes the embed only immediately before parsing it (existing behaviour, `EmbedSpawner.java:310`), so when `parseEmbedded` is entered for an embed at nesting depth *d*, the stack holds the root plus all `d-1` embed ancestors, i.e. `tikaDocumentStack.size() == d`. The guard:

```java
if (maxEmbedDepth > 0 && tikaDocumentStack.size() > maxEmbedDepth) {
    // refuse: record marker + counter, then return
}
```

With `maxEmbedDepth = 20`: embeds at nesting depth 1..20 are parsed normally; embeds at depth 21+ are refused. Intuitive reading: "up to 20 levels of embeds below the root." A depth-20 embed is fully parsed and spooled (it is within the limit); only its children (depth 21) are refused.

## On refuse

1. **No expensive work:** do not call `tis.getPath()`, do not `addEmbed`, do not `delegateParsing`. Return immediately from the spawn branch.
2. **Parent marker:** increment an integer marker on the parent document (`tikaDocumentStack.getLast()`), metadata key `X-EXTRACT:embedsSkippedMaxDepth`, holding the count of children refused directly under that parent. Read-parse-increment-set; the write is on the walk thread for that parent's subtree (single-threaded with respect to that parent, consistent with how other embed metadata is set during the walk). This produces an indexed, queryable flag on the container that swallowed the bomb.
3. **Progress counter:** bump a new per-file counter `embedsSkippedMaxDepth` on `ExtractionProgress`, null-checking `progress` first (the page-extraction path passes `null`, matching the existing `incrementEmbeds()` guard at `EmbedSpawner.java:240`).

## Configuration

New option on `Extractor`:

```java
@Option(name = "maxEmbedDepth", description = "Maximum nesting depth of embedded documents to " +
        "descend into. Embeds deeper than this are skipped (recorded, not parsed) to guard against " +
        "decompression-bomb archives. Defaults to 20. Set to 0 to disable.", parameter = "count")
```

- Field: `private int maxEmbedDepth = 20;` (also expose a `DEFAULT_MAX_EMBED_DEPTH = 20` constant for the convenience constructors).
- Parsed in `configure()`: `options.get("maxEmbedDepth", "20").parse().asInteger().ifPresent(n -> this.maxEmbedDepth = Math.max(0, n));`
- Getter `getMaxEmbedDepth()` for symmetry with the other options.

## Plumbing

- Add `maxEmbedDepth` as a parameter of `EmbedSpawner`'s master (widest) constructor.
- Thread it through the fork copy-constructor (`EmbedSpawner(EmbedSpawner template)`) so forks enforce the same cap on their own isolated DFS stacks. This matters for PST folder fan-out: each fork must reject deep embeds independently.
- The two convenience constructors (serial-mode and the `ExecutorService` overload) pass `DEFAULT_MAX_EMBED_DEPTH`.
- Wire the value in `Extractor.getTikaDocument` where the `EmbedSpawner` is constructed (`Extractor.java:951`).

## Byte-identity invariant

The guard is a pure no-op for any embed that is *not* refused: no code path for a within-limit embed changes. Therefore embed ids, `X-TIKA:digest:*` values, and artifact filenames are byte-identical to guard-off behaviour for every embed within the limit. Only embeds beyond the limit (which were previously indexed and are now deliberately skipped) differ.

## Testing

- Guard trips at the configured depth: an embed at depth `maxEmbedDepth + 1` is refused; an embed at `maxEmbedDepth` is parsed.
- Disabled when `maxEmbedDepth == 0`: arbitrarily deep embeds are all parsed.
- Refuse records both signals: parent metadata `X-EXTRACT:embedsSkippedMaxDepth` count and `ExtractionProgress.embedsSkippedMaxDepth`.
- Byte-identity: within-limit embeds produce identical ids / digests / names with the guard on vs off.
- Fork inheritance: a spawner produced by `fork()` enforces the same `maxEmbedDepth` against its isolated stack.
- Synthetic nested container (zip-in-zip beyond the limit) is refused at the boundary and does not spool the deep entry.
- `progress == null` (page-extraction path) does not NPE on refuse.

## Constraints / sequencing

- The guard modifies `EmbedSpawner`, which the unmerged `feat/pst-folder-fanout` branch also changes. Sequence this work *after* that branch merges, or rebase on it.
- Do NOT bump the extract version on the branch.
- Commit style: plain git, no Claude attribution.

## Out of scope (explicit)

- Per-entry byte cap, total-extracted-bytes budget (per-root or global), embed-count cap, compression-ratio guard.
- Enabling Tika's `maxEmbeddedResources`. (Trivial follow-up if a fan-out / many-tiny-entries bomb is ever observed.)
- Datashare surfacing of the new option (downstream follow-on).
