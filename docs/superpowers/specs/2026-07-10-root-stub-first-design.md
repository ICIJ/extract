# root-stub-first: make the container root visible at parse start

Date: 2026-07-10
Repos: ICIJ/extract (extract-lib) + ICIJ/datashare (datashare-index)
Status: design approved, ready for implementation plan

## Problem

When Datashare indexes a big OST/PST or archive with streaming spew, the container
root is written to the index LAST. Reading the root's content stream is what drives
the whole parse, so the foreground thread is blocked inside `writeDocument(root)` for
the entire parse and `indexer.add(root)` only completes when parsing finishes. For
hours-long OSTs the embedded children stream into the index while the parent/root is
absent, and operators (rightly) find that confusing. This is confirmed expected
behavior, not a bug.

## Goal

Make the container root visible in the index as soon as parsing starts working on it:
write a minimal contentless PARTIAL root stub at the first child, then enrich it to the
full COMPLETE root at parse end. This is the "root-stub-first" behavior that was scoped
but deliberately not shipped in the earlier orphaned-embeds work (only stub-on-abort +
finalize-on-complete shipped, extract 9.15.2 / datashare 21.14.0).

## Existing machinery (already in production)

- `Spewer.writeRootStub(root, writtenChildren)` (extract-lib default no-op override
  point). datashare override writes a contentless `RecoveryStatus.PARTIAL` root with
  `nbChildrenEmitted`, skips if the root id already exists (no clobber of a prior
  COMPLETE root), reads no content, uses the root's real id AND path.
- `Spewer.finalizeRoot(root, writtenChildren)` (extract-lib default no-op). datashare
  override partial-updates the finished root to COMPLETE + count for non-PST roots, and
  leaves PST rollup status (COMPLETE/PARTIAL/LOSSY) untouched.
- `StreamingSpewCoordinator`: worker thread drains embeds during the parse; `spew()`
  writes the real root then `finalizeRoot`s after `awaitDrained()` on the happy path;
  `close()` calls `writeRootStubIfOrphaned()` to stub the root only when the parse
  aborted before the real root was written.

## Key facts established from the code

1. The foreground thread is blocked inside `writeDocument(root)` for the whole parse, so
   the only thread that observes "the parse has begun emitting children" is the spew
   worker. The worker's existing first-child capture (`seenRoot == null`, coordinator
   line ~138) is exactly the "before the first child is written" moment.
2. The early stub is written with the same id AND same path as the real root. At parse
   end the foreground's real `writeDocument(root)` is therefore NOT treated as a
   `Duplicate` (`isDuplicate(id, path)` is false for same id+path), so it OVERWRITES the
   stub with full content/language/search text. The stub never leaves the root
   permanently contentless on normal completion.
3. On re-index, `writeRootStub` returns without writing because the prior COMPLETE root
   already exists, so an early PARTIAL stub never clobbers it. The real write then
   refreshes content and `finalizeRoot` marks it COMPLETE.
4. For PST roots, `applyParentRollup` runs during the real root write at parse end and
   sets the rollup status; `finalizeRoot` preserves it. The early stub carries generic
   PARTIAL, which the real write replaces.

## Design decision: aborted-root count (approved)

With an early stub, the abort path's `writeRootStub` re-call would skip (root already
exists), so an aborted root would keep the stale near-zero early count instead of the
accurate "children emitted before abort" count that the shipped feature records today.
Decision: PRESERVE the accurate count via a small datashare change. On abort, refresh
`nbChildrenEmitted` with a partial update that keeps PARTIAL status.

## Design

### extract-lib: `Spewer` API

- Change `writeRootStub(root, writtenChildren)` return type from `void` to `boolean`:
  `true` when a stub row was actually indexed, `false` when the call was a no-op / the
  root already existed. Default no-op returns `false`. This lets the coordinator know
  whether ITS OWN PARTIAL stub exists this run, so the abort count-refresh never touches
  a pre-existing COMPLETE root (the reindex-then-abort edge).
- Add `finalizeAbortedRoot(root, writtenChildren)` default no-op.

### extract-lib: `StreamingSpewCoordinator`

Add `volatile boolean rootStubWritten`.

- Early stub in `run()`: when `seenRoot` is first captured and the root is not a
  duplicate, before writing the first child, call
  `rootStubWritten = spewer.writeRootStub(root, writtenEmbeds.get())`. `writtenEmbeds`
  is 0 at this point; the count is corrected later. Best-effort: catch and log any
  failure so a stub write never breaks the parse.
- Happy path `spew()`: unchanged. The finalize gate stays keyed on `rootWritten` (the
  real full-content write), so `finalizeRoot` still runs only after a real root write
  and a full drain, and still skips on interrupt.
- `close()`: replace `writeRootStubIfOrphaned()` with an aborted-root handler:
  - if `rootWritten` return (real root already written; happy path already finalized);
  - resolve `root = seenRoot`; if null / duplicate / `writtenEmbeds == 0` return
    (nothing streamed under this root, no orphan risk);
  - if `rootStubWritten` -> `spewer.finalizeAbortedRoot(root, writtenEmbeds.get())`
    (refresh count, keep PARTIAL);
  - else -> `rootStubWritten = spewer.writeRootStub(root, writtenEmbeds.get())`
    (fallback when the early stub failed or the worker saw the root without an early
    stub having landed).

### datashare: `ElasticsearchSpewer`

- `writeRootStub`: return `false` on the two existing skip branches (`root.isDuplicate()`
  and `isDuplicate(root.getId())`), `true` after `indexer.add`. Logic otherwise
  unchanged.
- Add `finalizeAbortedRoot(root, writtenChildren)`: partial-update only
  `nbChildrenEmitted` via `indexer.update(indexName, root.getId(), fields)`. Do NOT
  write `recoveryStatus`, so the root stays PARTIAL. (`finalizeRoot` stays as-is: count
  + COMPLETE for non-PST, PST rollup preserved.)
- Bump `extract.version` to the release carrying the extract-lib change (datashare task
  gated on that release).

## Lifecycle summary

- Normal container: first child -> PARTIAL contentless stub (visible during the parse)
  -> children stream -> parse ends -> real root write (same id+path) overwrites stub
  with full content/language -> `finalizeRoot` -> COMPLETE + final count. PST: rollup
  set at the real write, preserved by finalize.
- Abort after stub: root stays PARTIAL, `finalizeAbortedRoot` refreshes the count to the
  drained value.
- Re-index: early `writeRootStub` returns `false` (prior COMPLETE not clobbered); real
  write refreshes content; finalize marks COMPLETE.

## Forward compatibility (resumable OST/PST/Archive parsing)

Resumable parsing may land later. Keep this design as its resume anchor at zero extra
cost now:

- The PARTIAL root + `nbChildrenEmitted` IS the record a resume would read ("which
  containers were mid-flight and how far they got"). We are already writing it.
- Keep ONE status vocabulary: `PARTIAL` means "incomplete root" regardless of cause
  (in-progress, aborted, crashed); a finished parse flips it to COMPLETE / PST rollup.
  Do not split PARTIAL into ABORTED/IN_PROGRESS variants: fewer states are less for a
  future resume to special-case.
- Keep `nbChildrenEmitted` as the count of durably-indexed children (stub, finalize, and
  abort-refresh all maintain it): the one datum a resume genuinely wants.
- Do NOT add a per-parser resume cursor now (archive entry index, PST descriptor/message
  id, byte offset). Those are parser-specific, not derivable generically at the
  coordinator, and belong to the resumable / progress track. No field is reserved now.
- Extension point: the single `writeRootStub` / `finalizeAbortedRoot` write path is where
  a resume cursor field would later attach, because it is the one place that already
  knows "root + how far." Resumable parsing extends this path rather than reworking it.

## Testing (TDD)

extract-lib `StreamingSpewCoordinatorTest` (`RecordingSpewer` fixture): update the
`writeRootStub` override to return `boolean` and record calls; add a `finalizeAbortedRoot`
recording override. New tests:

- happy path: a container emits children -> exactly one early `writeRootStub` recorded
  at/near the first child, then `finalizeRoot` recorded once at the end; no
  `finalizeAbortedRoot`.
- abort after stub: children streamed, real root never written, `close()` records one
  `finalizeAbortedRoot` with the final drained count and NO extra `writeRootStub`.
- reindex-abort no-clobber: `writeRootStub` returns `false` (root exists) so
  `rootStubWritten` stays false, and abort falls back to `writeRootStub` (which skips)
  rather than `finalizeAbortedRoot`, so a pre-existing COMPLETE root's count is never
  overwritten.
- duplicate root: no early stub, no finalize, children skipped (existing behavior).
- idempotency: only one early stub per parse even with many children.

datashare `ElasticsearchSpewerTest` (live ES): 

- early stub then real write: after a real root write over an existing stub (same
  id+path), the root has full content and is not a Duplicate.
- `finalizeAbortedRoot` refreshes `nbChildrenEmitted` and leaves `recoveryStatus` =
  PARTIAL (content, if any, preserved).
- `writeRootStub` returns `false` when the root id already exists; `true` on first write.

## Scope / non-goals

- No live count updates during the parse (that is the progress-heartbeat / #66 track).
  The early stub is a one-shot visibility write; the count is corrected only at finalize
  or abort.
- No new recoveryStatus values, no reserved resume-cursor field.
- Independent of PR #66 (progress-estimate), already merged to master; touches
  `StreamingSpewCoordinator`, `Spewer`, `ElasticsearchSpewer`, none of which #66 changed.
