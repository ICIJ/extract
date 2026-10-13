# OST/PST intra-parse folder fan-out (B3 increment 1) — design

Status: approved design, pending implementation plan
Date: 2026-06-29
Repos: extract-lib (primary) + datashare (thin adoption)
Branch base: `master` (extract), default branch (datashare). Stacks on B2
(`feat/streaming-spew-embeds` / `feat/streaming-spew-adoption`), still unmerged.

## Why

Box measurement on the prod OST box (2026-06-29, current B2 build,
real workload `--parallelism 9 --ocr true`) showed the instance is ~90% idle
while indexing the few-huge-files tail:

- 32 cores, load average **3.00**, CPU **90% idle**, 8.4 GB RAM used of 61 GB.
- Redis queue depth **0**; **3 OST files in flight**, each pinned to one parse thread,
  2819s (47 min) in. So 6 of 9 parse slots and ~29 cores sit idle.
- OCR fully caught up (heartbeats `65/65`, `8/8`, `40/40` done); 15+ `extract-ocr`
  pool threads all parked. Spew streaming continuously on `extract-spew`.
- Thread dump: all three parse threads in
  `ResilientOutlookPSTParser.walkFolder` / `walkSubFolders` / `emitFolderMessages`
  → `EmbedSpawner.spawnEmbedded` → `AutoDetectParser`/`DigestingParser.parse`.

Conclusion: the single serial Tika parse thread per file **is** the ceiling for the
few-huge-files case. B1 (parallel OCR) has nothing to do here because OCR is caught up;
B2 (streaming spew) works. The remaining wall-clock is the single-threaded PST folder
walk + inline attachment parse. This is what B3 attacks.

B4 (async OCR post-pass) was rejected for this step: it targets OCR latency / time-to-
searchable, but OCR is already caught up in this tail, so it would not lift the parse-tail
throughput that leaves the box idle.

## Scope (decided)

- **Option A only: intra-parse fan-out.** Parallelize the folder walk *inside* a single
  `Extractor.extract()` call. The queue item stays one `Path` per OST; datashare's scan,
  queue, `DocumentConsumer`, report, and on-demand download are **untouched**. The
  separate-queue-work-units variant (the roadmap's literal B3) is explicitly **not** in
  scope and stays a documented follow-up.
- **Granularity: recursive folders now, message-ranges later.** Every folder in the tree
  becomes a work unit; messages within a folder are walked serially. A single dominant
  flat folder (huge `Inbox`, no subfolders) is the known residual, deferred to a later
  message-range partitioning increment.
- End-to-end: extract-lib change + thin datashare adoption + box validation, in one effort.

## Sacred invariant

Embed ids / `X-TIKA:digest:*` / artifact filenames / `OCR_PARSER` must stay byte-identical
between serial and fan-out modes. On-demand download (`SourceExtractor`) and the artifact
cache depend on it, and on-demand re-walks the container **serially** to recompute ids, so
any walk-order dependency in identity would break download for the affected embeds.

### Why fan-out preserves it (grounded in the code + the corpus)

Embed id derivation (`DigestIdentifier.generateForEmbed`):

```
embedId = digest( own_content_hash + parent.getId() + embeddedRelationshipId + name )
```

- **Root stays the `.ost` file.** Fan-out does NOT re-root on a folder. Every walker emits
  messages whose `parent.getId()` is the single OST root id (datashare's
  `Document.getHash(project, path)` = `SHA-384(path, project)` — path+project based,
  identical for every walker, no content re-digest needed). One root doc, emitted once.
- **`embeddedRelationshipId`** for attachments is `descriptorId-index`
  (`ResilientOutlookPSTParser.formatRelationshipId`) — intrinsic to the PST structure,
  independent of walk order.
- **Messages always carry a name** (`subject + ".msg"`, or the fixed string `"untitled.msg"`
  when subject is null — not a counter) and no relationship id, so the message id is
  `digest(content + rootId + name)` — order-independent.
- **Nested embed names** (`image002.png`, `Microsoft_Excel_Worksheet1.xlsx`,
  `/docProps/thumbnail.emf`) are synthesized by the immediate container parser
  (POI/PDFBox/mail), numbered within one parent's parse — order-independent.
- **INLINE embeds are concatenated, never spawned** (`EmbedSpawner.parseEmbedded` line ~145),
  so they never get an id/name of their own.

The unit of concurrent work is a complete folder, and **each message-subtree (message + all
attachments + nested embeds) is parsed atomically on one thread**. No identity component
crosses a message-subtree boundary except one: `EmbedSpawner.untitled`, a global running
`untitled_%d` counter used only when a non-inline embed has a truly empty resource name.

Corpus evidence (a test index, 7245 docs, the real workload): embed depth tops out at
**level 3** (3 roots, 2451 messages, 4693 attachments, 98 nested); **zero** titles match
`untitled` — every embed, including the deepest, has a per-parent-deterministic name. So the
global counter effectively never fires for OST content.

### The one mitigation

Scope the `untitled` fallback **per immediate parent** (parent id + sibling ordinal under
that parent) instead of a global running counter, so it is order-independent and reproduced
identically by the serial on-demand walk even in the theoretical case it fires. This is cheap
insurance; the determinism test must exercise it with a genuinely nameless non-inline embed.

## Architecture

1. **Distribution.** `ResilientOutlookPSTParser` enumerates the folder tree (cheap
   descriptor-tree read, reusing the `PstFolderPathResolver` machinery) and submits one task
   per folder (recursively) to a **separate, shared, bounded parse pool**. Within a folder,
   messages are walked serially; across folders, parallel. The pool is global across all
   in-flight OSTs (so 3 concurrent OSTs share it; 1 remaining OST gets all of it) and sized
   by a new option, default ~CPU — mirroring B1's shared OCR pool. This is what fills the
   idle cores.
2. **Per-walker `EmbedSpawner`.** Each folder task gets its own spawner holding only per-walk
   state: a `tikaDocumentStack` seeded at the shared root, and the per-parent-scoped
   `untitled` index. The DFS stack is inherently single-thread (a shared one would hand a
   walker the wrong parent → wrong id), so it must not be shared. All heavy/shared
   collaborators are factored into a shared config the spawner references: the one root
   `TikaDocument`, the B2 `StreamingSpewCoordinator` sink, the B1 OCR `ExecutorService`, the
   `Digester`, the shared `TemporaryResources` (its `createTempFile` is already
   `synchronized(tmp)`), and a **shared** `AtomicLong` memory budget so `embedMemoryBudgetMb`
   is enforced across all concurrent walkers. Requires a small refactor: extract a spawner
   factory from `Extractor`'s `getTikaDocument` context wiring; serial mode is a factory of
   one and is behaviorally unchanged.
3. **Per-walker `PSTFile` handle.** `com.pff` is not thread-safe. Each folder task opens its
   own `PSTFile(pstPath)` and navigates to its folder by descriptor id. The OS page cache
   (52 GB on the box) serves the bytes; parallel reads of different regions are fine. Handle
   count is bounded by pool size — this is exactly where the roadmap's memory-headroom tuning
   applies.
4. **Thread-safe `EmissionContext`.** The dedup set (`HashSet<Long>`) becomes a concurrent
   set; `emittedCount` and the attachment-integrity counters become atomic. The root
   `XHTMLContentHandler` receives no content from a PST parse (spawned embeds write to their
   own handlers; only INLINE writes to the parent, and those stay within one message-subtree
   on one thread), so `startDocument`/`endDocument` only need to bracket the fan-out join.
5. **Serial tail.** After the pool joins, orphan recovery, `recordReconciliation`, and the
   attachment-integrity rollup run on one thread as today, reading the now-aggregated atomic
   counters into the root metadata.

### Spew coordinator: no change required

`StreamingSpewCoordinator` is already multi-producer safe — `promise()` is an `AtomicLong`,
`ready()` is a blocking `queue.put` on an `ArrayBlockingQueue`, the single-consumer worker is
unchanged, and `awaitDrained()` waits on `completed >= promised`. Its only requirement — all
promises made before `awaitDrained()` runs — holds automatically because `spew()`'s root-pipe
read returns only when `parse()` returns, and `parse()` joins all folder tasks before
returning.

### No deadlock

The B2 startup-ordering fix (worker started before `extract()`) still applies: the worker
drains to ES while folder tasks run. Folder tasks block only on spew-queue backpressure
(`queue.put`), which the worker always relieves (it never waits on a folder task). Deferred
OCR stays non-blocking (future + `sink.ready` on completion; submit does not block the walker).
Set `spewQueueCapacity` ≥ parse-pool size so producers don't needlessly serialize.

## Components touched (extract-lib)

- `ResilientOutlookPSTParser` — fan folder walk out to the parse pool; thread-safe
  `EmissionContext`; per-folder navigation by descriptor id; serial orphan/reconciliation tail
  after join; new fan-out toggle + pool plumbing.
- `EmbedSpawner` — split into shared config (root, sink, executor, digester, tmp, shared
  budget) + per-walk state (stack, per-parent `untitled`); add a factory/fork to mint a
  per-walker spawner; scope `untitled` per immediate parent.
- `Extractor` — wire the spawner factory and the parse pool into the parse context; add the
  parse-pool lifecycle (lazy create, closed on `Extractor.close()` like the OCR pool); new
  options.
- New options: `pstFolderFanout` (default **ON**, off-switch retained — B2 pattern) and a
  separate parse-pool size option (e.g. `pstParseParallelism`, default ~CPU), distinct from
  `ocrParallelism`.

## Testing (the guardian)

- **Determinism test** (sibling of `ParallelOcrDeterminismTest`): parse a PST/OST fixture
  with fan-out ON vs OFF; assert the **set** of `{embed id, X-TIKA:digest:*, artifact
  filename, name, OCR_PARSER}` over all embeds is identical. Fixture must include depth-3
  nested embeds and at least one genuinely nameless non-inline embed to exercise the
  per-parent `untitled` path.
- **Startup/deadlock guard** (sibling of `StreamingSpewPstStartupTest`): stream a PST through
  folder fan-out with a tiny spew queue and small parse pool; assert no deadlock and full
  emission.
- Full extract-lib suite stays green (modulo the known pre-existing native-Tesseract failures).

## Datashare adoption (thin, this effort)

- Bump `extract.version` to the new extract release (the same release that publishes B2's
  9.12.0; do not bump versions on feature branches).
- Expose `pstFolderFanout` + the parse-pool-size option, plumbed like `ocrParallelism`.
- Raise `embedMemoryBudgetMb` / `embedMemoryPressureThreshold` (and zlib-block-reader caches)
  for the higher concurrency — the roadmap's memory-headroom item, folded in here.
- Queue, scan, `DocumentConsumer`, report, and on-demand download unchanged.
- An adoption integration test analogous to `ElasticsearchSpewerStreamingTest` is optional;
  the byte-identity guarantee is carried by the extract-lib determinism test.

## Box validation

On the OST box with the real OSTs:
1. Baseline a single OST with `pstFolderFanout` OFF (serial) — record wall-clock and a sample
   of embed ids.
2. Re-run the same OST with fan-out ON — assert load climbs from ~3 toward the parse-pool
   size, the doc-count rate rises, and the sampled embed ids are unchanged.
3. Confirm no OOM (watch RSS vs the 61 GB box) and no deadlock (`kill -3` thread dump shows
   parse-pool threads active, not all parked/blocked).

## Rollout / flag

Ship behind `pstFolderFanout`, **default ON** with the off-switch retained (the streamingSpew
pattern), relying on the determinism test + the off-switch. Flip off to fall back to the
serial walk if the box surfaces a problem.

## Known limits (documented, not fixed here)

- A single flat dominant folder (huge `Inbox`, no subfolders) stays serial until message-range
  partitioning lands (deferred increment).
- Stacks on B2: extract `9.12.0` release + push, and the datashare
  `feat/streaming-spew-adoption` merge, are still pending.
- Unrelated pre-existing follow-up surfaced during B2 validation: multi-value
  `tika_metadata_signature_date` vs ES single-`date` mapping (`mapper_parsing_exception`).

## Decisions made during brainstorming (2026-06-29)

- Measurement is conclusive; design B3 now (not gather more / not B4).
- Scope = extract + datashare end-to-end; Option A (intra-parse fan-out) only.
- Granularity = recursive folders now; message-ranges later.
- Flag default = ON with off-switch (B2 pattern).
- Pool = separate bounded parse pool with its own size option, distinct from the OCR pool.
