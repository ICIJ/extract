# OST/PST folder fan-out (B3 increment 1) — code-review remediation design

Status: approved design (2026-07-01), pending implementation plan
Date: 2026-07-01
Repo: extract-lib (primary). Datashare on-demand wiring for #3 lands in the gated Task 7.
Branch: `feat/pst-folder-fanout` (base `master` @ d821bc6; HEAD 89c4da4)
Supersedes nothing; extends `2026-06-29-ost-folder-fanout-design.md`.

## Why

The branch was declared merge-ready by the per-task and whole-branch SDD reviews, then a fresh
multi-agent `/code-review max` found issues those reviews missed, chiefly a critical fork-isolation
bug that breaks the sacred byte-identity invariant. This is the remediation.

## Sacred invariant (unchanged)

Embed ids / `X-TIKA:digest:*` / artifact filenames / `OCR_PARSER` (and now also `PST_FOLDER_PATH`,
see #7a) must be byte-identical between serial and fan-out modes. Production uses `DigestIdentifier`
(SHA-384): `generateForEmbed = digest(own-content-hash + parent.getId() + embeddedRelationshipId +
name)`. On-demand download (`SourceExtractor`) re-walks the container SERIALLY to recompute an
embed's id, so any walk-order or thread-identity dependency leaking into an id breaks download.

## Fixes

### 1. CRITICAL — forked spawners do not isolate nested embeds (data race + wrong parent id)

Root cause (confirmed in code): `EmbedSpawner`'s fork copy-ctor does `super(template.root,
template.context)` — it SHARES the base spawner's `ParseContext`. The fork is placed only in
`EmissionContext.extractor`, used for the TOP-LEVEL message. But a message's own parse runs via
`delegateParsing(...)` against the shared `this.context`, whose registered
`EmbeddedDocumentExtractor` is the BASE spawner (`Extractor.java:935`). So every message ATTACHMENT
(depth>=2 — the bulk of embeds) is handled by the single base spawner from all folder threads
concurrently:
- data race on the base spawner's non-thread-safe `tikaDocumentStack` (LinkedList) and
  `untitledOrdinalsByParent` (HashMap);
- wrong parent attribution: the base stack holds only `[root]`, so an attachment gets `parent=root`
  instead of `parent=message` → wrong `parent.getId()` → wrong embed id/digest/artifact filename
  under `DigestIdentifier` → byte-identity broken, on-demand download misses.

Fix ("fork owns a cloned context"): each fork runs its nested parses against a
`ParseContext` where the fork ITSELF is the registered `EmbeddedDocumentExtractor`, so nested embeds
recurse into the same fork (correct DFS stack + own untitled map). Concretely, `fork()`'s copy-ctor
passes a cloned context to `super(...)` and, after construction, sets
`this.context.set(EmbeddedDocumentExtractor.class, this)`. The clone copies the parsing config
(`Parser`, `TesseractOCRConfig`, `PDFParserConfig`, `HtmlMapper`, `DocumentSelector`) via a helper
shared with the existing `buildIsolatedOcrContext()` (single maintenance point). The fork context
DELIBERATELY OMITS `PstFanoutConfig` (see #2). `ParseContext` (tika 3.3.0) keys entries by
class-name in a private map with no bulk-copy, so the copy is an explicit, known, closed set — the
same set `Extractor.getTikaDocument` sets.

Serial mode is unchanged: the single spawner is registered in the context and handles all depths as
before.

### 2. HIGH — reentrant pool-starvation deadlock on PST-in-PST — subsumed by #1

Because the fork context omits `PstFanoutConfig`, a nested `.pst/.ost` attachment parsed inside a
fork reads `context.get(PstFanoutConfig.class) == null` → `canFanOut == false` → serial `walkFolder`.
Only the OUTERMOST PST fans out; nested PSTs never re-submit to the bounded `pstParseExecutor` while
a parent folder task blocks. No thread-local guard needed. A guard test is added: a PST containing a
PST attachment parses with `pstParseParallelism=1` without hanging.

### 3. HIGH — `untitled` naming cross-version break — dual-resolution

The per-parent `untitled_<parentId>_<ordinal>` scheme (required for fan-out determinism) also changed
nameless-non-inline-embed naming in SERIAL mode vs the pre-branch release, so on-demand download of
OLD-indexed corpora recomputes a different id → 404 until re-index. Empirically 0 such embeds on the
real corpus, but we chose dual-resolution as insurance.

Mechanism (clean toggle, no metadata smuggling):
- extract-lib (this branch): add option `legacyUntitledNaming` (boolean, default `false`). When
  `true`, `EmbedSpawner` names nameless non-inline embeds with the RESTORED global `untitled_N`
  counter (the pre-branch scheme). Per-parent remains the default and the fan-out-deterministic
  primary. The legacy flag is only meaningful in SERIAL mode (global counter is deterministic only
  without fan-out), which is exactly where on-demand resolution runs.
- datashare (Task 7, gated on release): on-demand `SourceExtractor` resolves with the default
  (per-parent); on a miss, retries the serial re-walk with `legacyUntitledNaming=true`. The serial
  re-walk's global counter deterministically reproduces the legacy id for old corpora.

### 4. MEDIUM — `PstStdoutFilter` suppression does not cover pool threads

The libpst stdout-noise suppression is armed only on the parse thread; fan-out moves libpst work to
`extract-pst-walk-*` pool threads where it is inactive, so the ~11k lines of per-item noise return
on big OSTs. Fix: arm the depth-counter suppression (`PstStdoutFilter.begin()/end()` or
`runWithSuppressionLifted`) inside each folder task.

### 5. MEDIUM — handle-close-during-read on the interrupt/abnormal path

`cleanupParallelWalk` does `cancelFutures` (interrupt) then immediately `closeHandles`. On interrupt
a per-thread `PSTFile` can be closed while a task is still reading it → that folder's remaining
messages silently dropped. Fix: await task termination before closing handles (or have each task own
+ close its handle in its own `finally`).

### 6. LOW (batched)

- `awaitAllTasks` / interrupt handling: re-set `Thread.currentThread().interrupt()` where
  `InterruptedException` is caught, so interrupt status is not swallowed.
- `getOrOpenPstHandle`: `computeIfAbsent` does not cache `null`, so a thread whose open failed
  re-opens the `PSTFile` on every folder task. Cache the failure.
- `resolveFolderPaths`: compute lazily only when `canFanOut` or `messageDescriptorIds != null`
  (needed for the fan-out gate and orphan recovery), not unconditionally.
- `EmbedSpawnerForkTest` stack-isolation test: actually push onto the fork's stack and assert the
  base's depth is unchanged (the current test only asserts depth==1).

### 7. PLAUSIBLE — confirmed, fix + guard

- 7a `PST_FOLDER_PATH` divergence (REAL): serial uses the live `getSubFolders`/`getDisplayName`
  walk; fan-out uses `PstFolderPathResolver` descriptor-tree paths — different sources. Not part of
  the embed id, but it is indexed metadata that must be serial==fan-out identical. Add
  `PST_FOLDER_PATH` to the guardian test's identity tuple and reconcile the two sources if they
  diverge.
- 7b `getEmbeds()`/`hasEmbeds()` unsynchronized reads while `addEmbed` mutates under the `embeds`
  monitor: harden `getEmbeds()` to return a snapshot taken under the monitor (kills the CME risk
  permanently rather than relying on "no iterator exists today").
- 7c `TikaDocument.getId()` lazy memoization into a non-volatile `Supplier` field, realized
  concurrently by many fan-out threads on the now-SHARED root: harden the memoization to be safely
  published (synchronized/volatile holder). Deterministic today, but harden since the root is shared.
- 7d shared global `pstParseExecutor`: accept by design (documented). A per-mailbox cap is a separate
  tuning follow-up, not in scope.

## The guardian test (the whole point)

Rewrite the determinism test to actually catch #1:
- drive it with `DigestIdentifier` (production's SHA-384 identifier; the existing test used the
  default path-based identifier, which is why it missed the bug);
- over a fixture with attachments at depth>=2 (`testPST.pst` — 5 folders, messages with
  attachments; assert `folderPaths.size() > 1` so fan-out genuinely runs);
- assert the embed identity SET and exact COUNT are equal serial vs fan-out, where the identity
  tuple is `{id, X-TIKA:digest:*, resource-name, PST_FOLDER_PATH, OCR_PARSER}`;
- repeat the fan-out parse several times at `pstParseParallelism=8` to shake out races.

The test MUST fail on current HEAD (89c4da4) and pass after the #1 fix.

## Non-regression

- Full extract-lib suite green modulo the two known pre-existing native-Tesseract failures
  (`Tess4JOCRParserTest`, `ExtractorTest.test_ocr_confidence`).
- Serial mode provably unchanged for every fix (one spawner registered in context; legacy naming
  default off; snapshot/volatile hardening is uncontended in serial).
- Fresh `/code-review max` (concurrency + byte-identity focus) passes with finder agents confirmed
  to have actually run.

## Out of scope (documented follow-ups)

- Datashare adoption of `pstFolderFanout` / `pstParseParallelism` / `legacyUntitledNaming` +
  memory-headroom tuning + box validation (Task 7, gated on the extract release).
- Per-mailbox pool cap (#7d tuning).
- Message-range partitioning for a single dominant flat folder (deferred increment).
- `tika_metadata_signature_date` multi-value vs ES single-`date` mapping (datashare C1(b),
  separate).

## Decisions (2026-07-01)

- #1 fix shape = fork owns a cloned ParseContext registering itself as the EmbeddedDocumentExtractor;
  fork context omits `PstFanoutConfig` (also resolves #2).
- #3 backward-compat = dual-resolution via a `legacyUntitledNaming` flag (extract-lib) + on-demand
  fallback re-walk (datashare, gated).
- 7a/7b/7c = confirmed-fix (not document-only); 7d = accept by design.
