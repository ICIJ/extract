# OST-2013 Content-Layer Follow-ups (F3 / F4 / F5) — Design

> **Status:** design synthesis, approved 2026-06-25. Successor to `2026-06-25-ost-2013-attachment-recovery-design.md` (F1/F2, shipped on branch `feat/ost-2013-attachment-recovery`). Source: internal extract-indexing-robustness notes, §8 follow-ups.
> **Repo scope (decided):** extract-lib only on this branch; datashare and datashare-installer work captured as hand-off notes, not implemented here.
> **Principle carried from F1:** no reflection, no `--add-opens`, no fork of java-libpst. Same-package `com.pff` helpers reach package-public members at compile time on the classpath; this is the only mechanism used to touch java-libpst internals.

## Background

F1 (Strategy C, the pure-JVM per-block zlib reader `com.pff.OstCompressedBlockReader`) and F2 (drop the libpff hedge) are done and validated. The 48 GB-OST run that validated them used a *counting* extractor, so it exercised the reader and the folder walk but never the content/OCR layer. It surfaced three residual items — F3, F4, F5 — addressed here.

The single most important finding shaping this design: the big-OST run measured **0 net message loss** (`pst_expected == pst_emitted`, no loss WARN). Orphan recovery in `ResilientOutlookPSTParser` already re-reaches every message that a folder's `getNextChild` abandons. So none of F3/F4/F5 is about recovering lost messages; they are about *attribution*, *log legibility*, and *content-layer quality*.

## F3 — Correct folder-path attribution for orphan-recovered messages

### Problem

When `PSTFolder.getNextChild()` throws on a bad descriptor (the big-OST run hit 27 such folders, mostly `com.pff.PSTException: Unable to find <node> is desc: true`), `ResilientOutlookPSTParser.nextChild` (lines 205-212) stops that folder early. The folder's remaining messages are not lost — the descriptor-enumeration orphan pass (`recoverOrphans`) re-reaches them — but it stamps them with the sentinel folder path `/[recovered]` (`RECOVERED_FOLDER_PATH`) instead of their true folder path. This is the "structural" residual: data is present but mis-attributed.

### Why not "make getNextChild skip and continue"

`getNextChild`'s internal cursor `currentEmailIndex` is **private**, and the decompiled 0.9.3 bytecode shows it is incremented *after* descriptor resolution (`getDescriptorIndexNode` / `detectAndLoadPSTObject`). So when resolution throws, the cursor does not advance, and a naive catch-and-retry re-attempts the same bad child forever. The F1 same-package trick gives compile-time access to package-public members only — it cannot read or write a private field. Advancing the cursor would therefore require reflection (`setAccessible`), which violates the F1 no-reflection principle and would break under a JPMS module path. Reimplementing the per-folder content enumeration from scratch is also blocked: `emailsTable`, `fallbackEmailsTable`, and `initEmailsTable` are all private too. Both paths are rejected.

### Chosen approach — descriptor-tree path resolution

`DescriptorIndexNode.parentDescriptorIndexIdentifier` is a **public** field, and `PSTFile.getChildDescriptorTree()` exposes the full parent→children descriptor tree. This lets us compute a message's true folder path without touching the broken stateful iterator at all.

A new same-package helper `com.pff.PstFolderPathResolver` builds, once per PST, a `descriptorId → folderPath` map: for each folder descriptor (node type `NID_TYPE_NORMAL_FOLDER`), walk `parentDescriptorIndexIdentifier` up to the root, resolving each folder's display name, with every lookup wrapped in the existing `safe(...)` isolation so one corrupt folder name cannot abort the map build. `recoverOrphan` then resolves the recovered message's own `parentDescriptorIndexIdentifier` to a folder path through this map, and stamps that real path on the emitted message. `/[recovered]` is retained only as the fallback when resolution genuinely fails (no parent entry, or all name lookups failed).

This is robust by construction (per-node resolution, fully isolated), reflection-free, fork-free, and consistent with `OstCompressedBlockReader` and `PstMessageDescriptors`.

### Scope boundaries

- The happy-path folder walk (`walkFolder` / `emitFolderMessages` / `nextChild`) is unchanged; it works for the 99 %+ of folders and we minimize blast radius. We do not rewrite the walk to be descriptor-driven.
- The reconciliation metadata (`PST_EXPECTED` / `PST_EMITTED` / `PST_FAILED`) is already emitted on the root document. Making it queryable and alertable in the index is a **datashare hand-off** (see Hand-offs); no extract-lib change is needed for that half of the synthesis's F3 ask.

### Acceptance

- A message whose folder failed `getNextChild` but which is re-emitted by orphan recovery carries its real folder path, not `/[recovered]`, whenever its parent chain is resolvable.
- `/[recovered]` still appears only for genuinely unresolvable parents.
- No change to emitted message count or to reconciliation numbers (attribution only).
- Unit coverage in `ResilientOutlookPSTParserTest` for: resolvable parent chain → real path; unresolvable parent → `/[recovered]` fallback; corrupt folder name mid-chain → isolated, path still built from resolvable ancestors.

## F4 — Suppress java-libpst's direct `System.out` / `System.err` noise

### Problem

java-libpst writes ~1,100+ lines per big-OST parse directly via `println` to `System.out`/`System.err` (986× `Unknown message type: IPM.AbchPerson`, plus `Can't get children for folder…`, raw hex dumps, bare `---` / `4`). These bypass slf4j entirely, so logback cannot demote or filter them. None indicate loss, but they bury the recovery/reconciliation signal roughly 20:1 — the same legibility problem as the PDFBox-OCR font noise (F5).

### Constraint

The INDEX path runs N parallel parse threads (`DocumentConsumer` parallelism). `System.setOut` / `System.setErr` are JVM-global, so any redirection scoped naively "around a parse" would also swallow or corrupt output from sibling threads doing unrelated work.

### Chosen approach — thread-scoped filtering stream, installed once

A static initializer in `ResilientOutlookPSTParser` idempotently wraps `System.out` and `System.err` once with a filtering `PrintStream` that consults a `ThreadLocal<Boolean>` "inside-PST-parse" flag. `parse()` sets the flag around the `new PSTFile(...)` + extraction region and clears it in a `finally`. The filtering stream drops writes that originate on a thread whose flag is set, and passes every other write through to the original stream unchanged. Because the wrapper is installed exactly once and `PrintStream` writes are synchronized, this is thread-safe; because it is a transparent pass-through for all non-flagged threads, it has no effect on the rest of the JVM.

**Important:** because logback's `ConsoleAppender` resolves `System.out` lazily on every write, the blanket suppression window also gates any slf4j output routed through a console appender — including the parser's own per-item recovery INFO lines and any embedded PDFBox/OCR slf4j output — on the console during the suppressed region. Durable per-item diagnostics survive via the FILE appender and the ES-indexed `pst_*` metadata produced by the F3 hand-off.

To keep the load-bearing per-PST reconciliation summary visible on the console, `ResilientOutlookPSTParser` calls `PstStdoutFilter.end()` immediately before `recordReconciliation` and `recordAttachmentIntegrity`, lifting suppression before those WARN lines are emitted. Per-item recovery INFO lines (~11k on a large OST) remain suppressed on the console — they are noise at scale. The `parse()` `finally` block still calls `end()` as an idempotent safety net (`ThreadLocal.remove()` is a no-op on an already-cleared slot).

**Decision:** full suppression on the parse thread (drop all `System.out`/`System.err` writes while the flag is set), not pattern matching. Rationale: the synthesis established that none of java-libpst's stdout/stderr output indicates loss, and per-item recovery INFO lines are high-volume noise at scale. The load-bearing per-PST summary is emitted with suppression lifted. This avoids a brittle, maintained pattern list.

### Scope boundaries

- Installing the wrapper is a one-time, idempotent, transparent global side effect from a library — acceptable because it is pass-through for everything except the narrow flagged region, and it is the only way to intercept output that bypasses slf4j.
- We do not attempt to restore the original streams (the wrapper is harmless when idle); install-once with a guard against double-wrapping.

### Rejected

- Per-parse `System.setOut`/`setErr` — unsafe under concurrent parses.
- Unconditional global redirect to null — drops unrelated stdout from the host process.
- Pattern-matched line filtering — brittle, needs maintenance, and unnecessary given full thread-scoped suppression is safe here.

### Acceptance

- A PST parse emits zero java-libpst `println` lines to stdout/stderr; slf4j output is unaffected.
- Output from a concurrent non-PST thread during a PST parse is unaffected.
- Wrapper install is idempotent (multiple parser instances / repeated parses do not stack wrappers).

## F5 — Content / OCR layer: codeable fixes + measurement runbook

The counting-extractor run never exercised real Tika parsing, OCR, or the spewer, so the content-layer error profile is still unmeasured. F5 splits into what is codeable now and what needs the full run.

### F5a — Demote PDFBox font-noise (codeable now, harness-side)

`PDFStreamEngine: No current font` and the related font-fallback warnings (`SetFontAndSize`, `GlyphCache`, `PDTrueTypeFont`) are ~95 % of WARN volume during OCR. They originate inside PDFBox, not extract-lib code, so the only lever is logger-level configuration: raise the relevant PDFBox logger names to ERROR. This lands in `extract-cli`'s logback (`extract-cli/src/main/resources/logback.xml`) for immediate harness legibility, and is handed off for datashare's runtime logback. (Verify in implementation whether PDFBox 3.x in Tika 3.3.0 logs via slf4j logger names amenable to logback, and capture the exact logger names; this is the JBIG2→DEBUG demotion pattern from 9.9.0, but applied via config since the call site is inside PDFBox.)

### F5b — Encrypted-attachment residual (codeable now, documentation)

`recoverAndEmitAttachment` already catches `EncryptedDocumentException`, indexes the byte-perfect attachment, and increments `PST_ATTACHMENTS_ENCRYPTED`. The known caveat (documented in code and commit 61ef3a6): under Tika's default `ParsingEmbeddedDocumentExtractor` the counter reads 0 because that extractor swallows `EncryptedDocumentException` internally. Scope for this branch: document the encrypted attachment as a **distinct residual class** (byte-recovery succeeds, body text needs credentials) in the parser docs and the measurement runbook. The password/credential extraction path is explicit future work, not implemented here. Optional, deferred: pre-parse encryption sniffing on recovered bytes to make the count accurate despite Tika's swallow — out of scope unless the measurement shows it matters.

### F5c — OCR recall for the corpus language (hand-off, datashare-installer)

`<lang>` tessdata is not installed, so `AUTO` OCR on this non-English mailbox runs but yields poor text — a recall gap, not a logged error. Install `<lang>` tessdata via datashare-installer's per-channel native-dependency model (apt / brew / snap stage-packages), per `ocr-language-packages`. Hand-off only.

### F5d — Measurement runbook (deferred execution)

A committed runbook to execute once the 48 GB OST + an OCR environment with `<lang>` are available. It must specify: the exact invocation (real `Extractor` → SHA-256 → `ResilientOutlookPSTParser`, `embedHandling=spawn`, OCR `AUTO`, `eng`+`<lang>`, ergonomic heap, output to a real spewer or `/dev/null` for insight); what to capture (PDFBox parse-failure profile and counts, OCR recall/quality on the recovered JPEGs/PDFs, the encrypted-attachment count behaviour, and reconciliation deltas `pst_expected`/`pst_emitted`/`pst_failed` plus `pst_attachments_*`); and pass/fail criteria per signal. The runbook is the deliverable; running it is out of session scope.

## Where the code lands

- **extract-lib (this branch, `feat/ost-2013-attachment-recovery`):**
  - `com.pff.PstFolderPathResolver` + `recoverOrphan` wiring in `ResilientOutlookPSTParser` (F3).
  - Thread-scoped stdout/stderr filtering stream + `ThreadLocal` flag in `ResilientOutlookPSTParser` (F4).
  - Encrypted-residual documentation in the parser docs/comments (F5b).
  - Tests in `ResilientOutlookPSTParserTest` for F3 attribution and F4 suppression.
- **extract-cli:** PDFBox logger demotion in `logback.xml` (F5a, harness).
- **Hand-off notes (not implemented here):**
  - datashare: index/expose `pst_expected`/`pst_emitted`/`pst_failed` and `pst_attachments_*` so reconciliation is queryable and alertable (F3 second half).
  - datashare: mirror the PDFBox logger demotion in runtime logback (F5a).
  - datashare-installer: install `<lang>` tessdata (F5c).
  - Measurement runbook for the full content/OCR pass (F5d).

## Out of scope

- Touching `getNextChild` / forking or reflecting into java-libpst (F3 is solved via the public descriptor tree instead).
- The §5 process-isolation supervisor and §6 ledger from the parent memo (separate, larger effort).
- Password/credential extraction for encrypted attachments (future residual-class work).
- Running the F5d measurement (needs the 48 GB OST + OCR env).
