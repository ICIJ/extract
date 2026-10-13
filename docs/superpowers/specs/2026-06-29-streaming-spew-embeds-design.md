# Streaming spew: write embeds as they are parsed (B2)

Date: 2026-06-29
Repo: `extract` (extract-lib) + `datashare` (adoption)
Status: design approved, pending implementation plan

## Problem

Today `Extractor.doExtract` parses a whole container into an in-memory
`TikaDocument` tree and only then walks and writes it:

```
TikaDocument document = extract(path);   // background parse builds the WHOLE tree
spewer.write(document);                  // walk the tree, write root then every embed
```

For a 21-48 GB Outlook OST that means:

- **Silent for hours at the source.** The Elasticsearch document count does not
  move until the entire mailbox is parsed, so the box looks hung from the
  indexing side even though the parse is healthy. (B1/A1 added a log heartbeat;
  this addresses the actual doc-count visibility.)
- **Memory holds the whole tree.** Every embed's extracted text is buffered (in
  memory up to the budget, then spilled to temp files) and retained until the
  post-parse walk, so peak retention scales with the mailbox.
- **A mid-parse failure discards everything.** If the parse throws partway
  through a giant mailbox, nothing was written, so the whole file is lost and
  re-done from scratch.

## Goal

Spew each embedded document as it is produced during the parse, instead of
buffering the whole tree and writing it afterwards. Wins:

1. ES doc count climbs **during** the parse (kills the "silent for hours" look).
2. Per-embed text buffers are freed as soon as each embed is written, so peak
   memory tracks in-flight work rather than the whole tree.
3. A mid-parse failure leaves the already-written embeds in the index as partial
   progress instead of discarding them.

## Non-goals

- Splitting a container into multiple parallel work units (B3, separate change).
- An async OCR post-pass driven by Datashare's task queue (B4, Datashare-side).
- Dropping tree node skeletons mid-parse (see Memory: deferred, safe follow-up).
- Changing the digest / embed-ID / artifact-filename computation in any way.

## Sacred invariant (unchanged, preserved by construction)

Embed IDs, `X-TIKA:digest:*`, and artifact filenames stay **byte-identical** to
today. They are computed **synchronously on the parse thread** before any spew:

- non-deferred embeds: `DigestingParser` digests during `delegateParsing`;
- deferred (parallel-OCR image) embeds: `spawnEmbeddedDeferred` digests the
  parse-owned byte copy **before** submitting OCR (`EmbedSpawner` ~line 300).

Spew timing never feeds back into these. On-demand `SourceExtractor` /
`EmbeddedDocumentExtractor` and the `artifactDir` cache resolve by these values
and are unaffected. The design adds no write to the digest/ID path.

## Architecture

### Threading model (approach A: one dedicated spew worker)

| Thread | Role |
|--------|------|
| **Parse thread** (`ParsingReader`'s background daemon) | Runs the parse. Builds each embed node, digests, writes artifact, all synchronously as today. For a **non-deferred** embed, after its text is buffered, enqueues a `SpewItem(embed, parent, root, level)` to the spew queue. For a **deferred** embed, submits OCR as today and attaches `done.whenComplete(...)` to enqueue the same `SpewItem` when OCR finishes. |
| **OCR pool** (B1's shared bounded pool) | Unchanged. Its existing completion (`done.complete(null)`) now also triggers that embed's enqueue. Never calls the spewer. |
| **Spew worker** (NEW; one per extraction) | Sole consumer of the queue. For each `SpewItem`: if `root.isDuplicate()` skip; else `spewer.writeDocument(embed, parent, root, level)`; then close that embed's reader to free its text buffer / spill file. Runs serially, so embeds are written one at a time. |
| **Foreground** (watchdog thread running `doExtract`) | Calls `spewer.writeDocument(root, …)` (today's root path): determines the root's duplicate status early, drains the root pipe (which drives the whole parse and thus all embed enqueues), and indexes the root after its full text is read. Then awaits the worker drained and closes the root reader (temp cleanup). |

Invariants that make this safe and deadlock-free:

- **The worker never blocks on OCR.** A deferred embed is enqueued only *after*
  its OCR future has completed, so the `join(done)` inside its reader generator
  returns immediately when the worker reads it.
- **Bounded queue = backpressure, not deadlock.** A full queue blocks the parse
  thread on enqueue; the worker always makes progress on dequeued items (their
  text is ready), so it drains and the parse resumes. The foreground only ever
  waits on the pipe and, at the end, on the worker — never the reverse.
- **Out-of-order / post-order spew is correct.** Embeds are independent ES docs
  keyed by content hash; `writeDocument(child)` needs only `parent.getId()` /
  `root.getId()`, both set synchronously at spawn time. A nested container embed
  is naturally written after its children (post-order); order does not matter to
  the index.

### Duplicate-root gating (preserved)

`ElasticsearchSpewer.writeDocument` sets `root.setDuplicate(true)` and skips all
children when the root is a byte-duplicate of an already-indexed file. This
ordering dependency survives because `root.getId()` is set by `DigestingParser`
**before the first `parseEmbedded` callback**, and the foreground's
`writeDocument(root)` performs the duplicate check **before** it drains the pipe
(i.e. before the parse produces any embed). So `root.isDuplicate()` is decided
before the worker dequeues its first child. To guarantee cross-thread
visibility of the flag (set by the foreground, read by the worker), the
coordinator publishes the decision through a `volatile` field rather than
relying on `TikaDocument` field visibility.

### Root write: two-writer model

The foreground keeps calling `writeDocument(root, null, null, 0)` exactly as
today (duplicate check, drain pipe = drive parse, index root after the full
read). The worker writes embeds concurrently. There is a tail window where
`indexer.add(root)` (foreground) can overlap a few in-flight `indexer.add(child)`
(worker), so `ElasticsearchSpewer` must tolerate concurrent `writeDocument`
calls. This holds today:

- `indexer.add` goes through the Elasticsearch bulk processor (built for
  concurrent submission);
- the Redis output queue's `offer` is thread-safe;
- `OptimaizeLanguageGuesser` wraps an immutable, thread-safe Optimaize
  `LanguageDetector`;
- `getMetadata` builds a per-call local map.

The implementation plan **verifies** this with a concurrency test and wraps any
call found not to be thread-safe. (Chosen over a single-writer refactor that
would split `ElasticsearchSpewer`'s root handling, because the bulk indexer is
already concurrent and the datashare diff stays minimal.)

### Component placement

- **extract-lib**
  - Keep `Spewer.write(TikaDocument)` (the tree walk) intact: it still backs
    `FileSpewer`, page extraction, `CONCATENATE`/`IGNORE` handling, and is the
    fallback when streaming is off.
  - New `StreamingSpewCoordinator` (queue + single worker thread + lifecycle:
    start, `enqueue`, `awaitDrained`, `shutdown`). One per extraction, created
    lazily only for the `SPAWN` + streaming path.
  - `EmbedSpawner` gains an `onEmbedReady` callback (a `Consumer<SpewItem>` or
    small sink interface). `spawnEmbedded` calls it after `delegateParsing`;
    `spawnEmbeddedDeferred` wires it onto `done.whenComplete`. When no sink is
    set (non-streaming callers), behaviour is exactly as today.
  - `Extractor.getTikaDocument` wires the sink when `embedHandling == SPAWN` and
    streaming is on; `doExtract` drives the foreground sequence and owns the
    coordinator lifecycle.
  - New `streamingSpew` option (`@Option`), default **ON** so the change is live
    and measurable on the box, flippable to fall back to the tree walk.
- **datashare**
  - `ElasticsearchSpewer.writeDocument` unchanged (already the per-doc
    primitive). Bump `extract.version`. Add the concurrency test. Validate on
    the OST box.

## Memory

The worker closes each embed's reader as soon as it is written, freeing the
dominant cost (text buffers and spill temp files) promptly — this is the real
"memory stays flat" win. Tree **node skeletons** (text-free metadata maps) are
**not** dropped mid-parse: mutating a parent's embed list while the parse thread
is still adding to it would race. Skeletons are retained until the extraction
ends and GC'd then. At 8/32 GB heap this is a non-issue; safely dropping spewed
subtrees is a documented future optimization, not part of B2.

Once spew is streaming and per-embed retention is bounded, the cross-cutting
memory-headroom tuning (`embedMemoryBudgetMb`, `embedMemoryPressureThreshold`,
zlib caches) can be revisited — tracked with B2 but tuned after measurement, not
designed here.

## Failure and retry

- A mid-parse failure throws through the foreground's root read. Embeds already
  written **stay** in the index (the intended partial progress). On failure the
  foreground still drains/shuts down the worker (no thread leak) and closes the
  root reader (temp cleanup). Queued-but-unwritten embeds are **flushed** before
  shutdown (idempotent, and yields more partial progress) rather than dropped.
- The `Reporter` records status **per root path** (`Reporter.skip` is keyed on
  the root path), not per embed. A recoverable mid-parse failure leaves the root
  path retryable; a re-run re-extracts the whole file and **overwrites every
  embed by ID** (idempotent upsert, since the ES doc ID is the byte-identical
  content hash). No duplicates, no per-embed bookkeeping. The only requirement —
  byte-identical embed IDs across runs — is already sacred.
- `ResilientOutlookPSTParser` still swallows per-message failures and continues,
  so a single bad message does not abort the mailbox. Fatal errors
  (OOM/`StackOverflowError`) still exit the process for a clean restart, after
  which the retryable root path is re-extracted.

## Testing

1. **Byte-identity (regression guard).** For a representative container, assert
   embed IDs, `X-TIKA:digest:*`, and artifact filenames are identical between
   the streaming path and the legacy tree walk. (Extends the existing
   serial-vs-parallel-OCR identity test.)
2. **Streaming order / progress.** Assert embeds are written before the root and
   before the parse-complete signal (e.g. a recording spewer observes embed
   writes interleaved with parse progress, not all after EOF).
3. **Deferred OCR.** With fan-out on, assert deferred embeds are written after
   their OCR completes, the worker never blocks waiting on OCR, and OCR-derived
   text/`OCR_PARSER` match the legacy path.
4. **Mid-parse failure.** Inject a failure partway through a multi-embed
   container; assert already-written embeds persist, the root path is recorded
   retryable, the worker and root reader are cleaned up, and a re-run overwrites
   (no duplicate docs).
5. **Concurrency.** Assert `ElasticsearchSpewer` tolerates concurrent
   `writeDocument` calls (root overlapping children) without corruption — the
   two-writer safety check.
6. **Backpressure / no deadlock.** With a small queue bound and a slow spewer,
   assert the extraction completes (parse blocks and resumes; no deadlock).
7. **Non-streaming fallback.** With `streamingSpew` off, behaviour and output are
   identical to today (tree walk).

## Adoption / sequencing

Per the decision to ship end-to-end (like A3), datashare adoption is **in this
effort**: bump `extract.version`, add the concurrency test, validate on the
OST box (`kill -3 <pid>` for thread dumps into journald)
against the 21/48/38 GB OSTs, confirming the ES doc count climbs during the
parse and peak memory is bounded. After B2 is measured, decide B3 (per-folder
work units) vs B4 (async OCR post-pass).

## Constraints / preferences

- ff/rebase, no squash; commit via `/commit`; no Claude attribution; branched
  off the default branch (`feat/streaming-spew-embeds`).
- extract-lib tests via `mvn` in the extract repo.
- Datashare single-test: `mvn -pl datashare-app -am test -Dtest=X
  -Dsurefire.failIfNoSpecifiedTests=false`; ES `elasticsearch:9200`, Redis
  `redis:6379`.
