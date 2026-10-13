# Parallel OCR and extraction progress visibility

Date: 2026-06-29
Repo: `extract` (extract-lib)
Status: design approved, pending implementation plan

## Problem

Indexing large Outlook OST mailboxes makes Datashare look hung and leaves the
machine almost idle. Observed on a 32-core / 61 GB box indexing six OST files
(three of them 21-48 GB):

- **Idle look.** Tika parses each container as a single document on one thread
  and only spews children *after* the whole file is parsed
  (`Extractor.doExtract`: `extract(path)` then `spewer.write(document)`). A
  multi-hour parse of a 48 GB OST emits no log lines. The only output was
  `DocumentConsumer`'s generic `Executor failed to terminate in 30 MINUTES`
  heartbeat, which reads as a hang.
- **Idle capacity.** The unit of parallelism is the whole file, and OCR runs
  inline and serial inside the single parse thread (`EmbedSpawner` ->
  `delegateParsing` -> `TesseractOCRParser`). With three files left, only three
  of 32 cores were busy (load avg ~3). Heap sat at 8 of 32 GB.

A thread dump confirmed the three big-file threads were healthy but each parked
in `TesseractOCRParser.runOCRProcess`: OCR was the dominant cost, run one image
at a time per mailbox.

## Goals

1. **B1** Parallelize OCR so a single mailbox's image attachments fan out across
   cores, with one shared bounded pool that also caps total concurrent OCR
   regardless of how many files are in flight.
2. **A1** Emit a periodic progress heartbeat (operator-facing log) and expose a
   programmatic progress hook so Datashare can surface progress later.
3. **A2** Replace the misleading await message with one that names the
   still-running files and their elapsed times.

All work lands in `extract-lib`. Datashare consumes it by bumping
`extract.version`; the features function with no Datashare code change. Two
optional Datashare follow-ups: register a `ProgressListener` to surface progress
in the UI/task status (A3), and call `Extractor.close()` per task in server mode
so OCR pools do not accumulate across repeated `IndexTask`s (see below).

## Non-goals

- Streaming embeds to the index as they are parsed (separate change).
- Splitting a container into multiple work units (separate change).
- An async OCR post-pass driven by Datashare's task queue (Datashare-side).
- Parallelizing PDF render-OCR (`ocrStrategy`); only image-leaf OCR fans out.

## Shared component

One new collaborator backs all three features so they do not each grow their own
bookkeeping.

- `ExtractionProgress` (`org.icij.extract.extractor`) — per-file mutable
  snapshot: `path`, `startMillis`, and atomic counters `embedsParsed`,
  `ocrSubmitted`, `ocrCompleted`. Read-only getters for listeners.
- `ExtractionProgressTracker` — owns a `ConcurrentMap<Path, ExtractionProgress>`
  of in-flight extractions, a single daemon `ScheduledExecutorService` firing
  every `progressHeartbeatInterval`, and a list of `ProgressListener`s.
  `begin(path)` registers and returns the handle; `end(path)` removes it.
  Knows nothing about OCR or Tika.
- `ProgressListener` — `void onHeartbeat(Collection<ExtractionProgress>)`.
  `LoggingProgressListener` is the built-in first listener.

`Extractor` holds one shared OCR `ExecutorService` and one
`ExtractionProgressTracker`, both created once and shared across all parse
threads (there is a single `Extractor` per `IndexTask`). `Extractor` becomes
`AutoCloseable` to shut them down; the tracker's scheduler and the OCR pool use
daemon threads so a CLI process can still exit if `close()` is missed.

Data flow: `DocumentConsumer.accept` -> `extractor.extract(path)` ->
`tracker.begin(path)` -> the per-file `ExtractionProgress` and the OCR executor
are placed in the per-parse `ParseContext` -> parse (`EmbedSpawner` bumps
counters, submits OCR futures) -> join futures -> `spewer.write` -> `tracker.end(path)`.
The heartbeat thread reads the live map every interval and feeds all listeners.

## B1 — parallel OCR

### Shared pool

`Extractor` owns `ExecutorService ocrExecutor =
Executors.newFixedThreadPool(ocrParallelism)` (default `availableProcessors()`),
created in the constructor, shut down in `close()`. Because there is one
`Extractor` across all parse threads, this single pool is simultaneously the
cross-file cap and the within-file fan-out: total concurrent OCR is at most
`ocrParallelism` no matter how many mailboxes or images are in flight.

### Eligibility

Only **leaf, OCR-eligible image embeds** are deferred: `CONTENT_TYPE` starts
with `image/`, the embed is not `INLINE` (those already concatenate today), and
it is not a container. Everything else — nested `.msg`/zip containers, office
docs, text, PDFs — stays on today's inline path. `ocrMinImageBytes` (default 0)
lets tiny images (signature logos) skip the deferral and parse inline.

### Mechanism (in `EmbedSpawner.spawnEmbedded`)

The key correctness constraint: **digest synchronously, OCR asynchronously**.
The embed's content hash (`X-TIKA:digest:*`) is currently a side-effect of
`delegateParsing`, and the embed ID (`EmbeddedTikaDocument.generateId` ->
`DigestIdentifier.generateForEmbed`) plus `writeEmbed`'s artifact filename
(`embed.getHash()`) depend on it. Deferring the whole parse would defer the
digest and break embed identity, artifact storage, and Datashare's on-demand
re-extraction. Therefore:

1. On the walk thread, build the embed node synchronously exactly as today
   (`parent.addEmbed(metadata)`, name resolution, stack push/pop). Tree
   structure, ordering, and IDs are unchanged.
2. Spool the embed bytes to a temp file via the existing `TemporaryResources`
   (the input stream is only valid during the call).
3. Run the **digester over the spooled bytes synchronously** and set
   `X-TIKA:digest:*` on the embed. Embed ID and `writeEmbed` are then computed
   on the walk thread, byte-identical to serial mode.
4. Submit only the **OCR text extraction** to `ocrExecutor`. The task parses the
   spooled image through the OCR parser into the embed's `BudgetedEmbedBuffer`
   and sets `OCR_PARSER` on the embed metadata. `ocrSubmitted++` on submit,
   `ocrCompleted++` in `finally`.
5. Back the embed's lazy reader with the OCR `Future` so a read blocks until OCR
   completes. The walk thread returns immediately and proceeds to the next embed.

### Joining

`Extractor.extract` collects the per-root list of OCR futures (parse-scoped, via
the `ParseContext` or `ExtractionProgress`). After the synchronous parse returns
and before `spewer.write`, it awaits all futures, bounded by the existing
`parseTimeout` / `ocrTimeout`. The future-backed reader is the backstop so spew
can never read empty text. Joining before spew guarantees the `OCR_PARSER`
field and text are present when the embed is indexed.

### Compatibility with on-demand extraction

Datashare's `SourceExtractor.getEmbeddedSource` re-opens the root and walks
serially via extract-lib `EmbeddedDocumentExtractor.extract` to find one embed
*by its ID*, recomputing the digest over the raw bytes. The artifact cache keys
files by `embed.getHash()`. On-demand OCR is re-enabled only when
`document.getOcrParser() != null`.

This path is **not modified**. Because the bulk path now produces byte-identical
embed IDs and the same `OCR_PARSER` field, later extraction and the `artifactDir`
cache continue to match. Only the embed's searchable *text* is produced in
parallel; its *identity* stays on the deterministic serial path.

### Safety

- **No deadlock.** OCR tasks are leaf image parses; they never resubmit into
  `ocrExecutor` or the parse pool, so a full pool cannot starve itself. Parse
  threads blocking on `Future.get` yield while the pool does the CPU work.
- **Memory.** At most `ocrParallelism` images in flight, each spooled to temp
  (disk, not heap). `embedMemoryBudgetMb` / `embedMemoryPressureThreshold` still
  bound in-memory extracted text.
- **Errors / timeouts.** A failed or timed-out OCR future is caught per-embed
  and recorded as `TIKA_META_EXCEPTION_EMBEDDED_STREAM`, exactly today's
  behavior; never fatal to the mailbox. With parallel OCR, `ocrTimeout` (12h)
  bounds how long a wedged tesseract holds a pool slot — consider lowering it
  operationally.

## A1 — heartbeat and progress hook

The tracker's daemon scheduler fires every `progressHeartbeatInterval`
(default 60s), snapshots the in-flight map, and invokes every `ProgressListener`.
`LoggingProgressListener` emits one INFO line per in-flight file, for example:

```
small.ost: 6m12s, 412 embeds, 88/91 OCR done
```

Counters are bumped by `EmbedSpawner`: `embedsParsed` on `addEmbed`,
`ocrSubmitted` on submit, `ocrCompleted` in the task `finally`.

`Extractor.addProgressListener(ProgressListener)` (delegating to the tracker)
lets Datashare register a listener that pushes to its task/UI progress later,
with no further extract change.

## A2 — enriched await message

`DocumentConsumer.awaitTermination` stops delegating its logging to the generic
`ExecutorProxy` message. It awaits the executor directly; on timeout it logs a
WARN listing each still-running file and its elapsed time from the tracker, then
closes the spewer. The await window stays caller-controlled (Datashare's
`IndexTask` 30-minute loop); the 60s heartbeat covers the gaps.

## Parameters

All new behavior is gated by an `@Option` on `Extractor`, mirrored as a
Datashare CLI option.

| Option | Type | Default | Effect |
|---|---|---|---|
| `ocrParallelism` | int | `availableProcessors()` | Shared OCR pool size = degree of fan-out. `1` restores exactly today's serial behavior. Lower to leave CPU headroom. |
| `ocrFanout` | boolean | `true` | Master kill-switch. `false` -> images OCR inline on the walk thread regardless of pool size. |
| `ocrMinImageBytes` | long | `0` (off) | Images smaller than this skip deferral and parse inline. Perf knob; does not change whether OCR runs. |
| `progressHeartbeatInterval` | duration | `60s` | Heartbeat cadence. `0` disables the heartbeat thread. |

Interacting existing options: `ocrTimeout` (12h) now bounds how long a wedged
tesseract holds a pool slot; `embedMemoryBudgetMb` / `embedMemoryPressureThreshold`
bound in-memory embed text under higher concurrency.

## Testing

- **On-demand contract (most important).** Parse an image-bearing fixture with
  `ocrParallelism=1` and `=8`; assert the document tree, embed IDs, and
  `X-TIKA:digest:*` are byte-identical. Round-trip: index with parallel OCR,
  then resolve a specific embed by ID via `EmbeddedDocumentExtractor.extract`
  (the path `SourceExtractor` uses) and assert it is found. Proves later
  extraction and the `artifactDir` cache still match.
- **Concurrency.** A stub OCR parser (sleep + record active count) proves more
  than one OCR runs at once and that the ceiling equals `ocrParallelism`; keeps
  tesseract out of CI.
- **Eligibility.** Image leaf -> deferred; container / `INLINE` / text / PDF ->
  inline.
- **Tracker.** begin/end membership; a manual tick invokes listeners;
  `ocrFanout=false` and `progressHeartbeatInterval=0` disable cleanly.

## Backward compatibility

- `ocrParallelism` defaulting to CPU count changes behavior on upgrade (more
  concurrent tesseract processes, higher transient memory and temp-disk use).
  `ocrParallelism=1` or `ocrFanout=false` restores exact serial behavior.
- The heartbeat is additive logging; the progress hook is opt-in API.
- Embed IDs, digests, artifact filenames, and the on-demand extraction path are
  unchanged by design and asserted by test.
