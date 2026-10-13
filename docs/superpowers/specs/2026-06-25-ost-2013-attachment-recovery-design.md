# OST-2013 attachment recovery — Strategy C (pure-JVM zlib block reader)

- Status: design approved (brainstorm), pending spec review → implementation plan
- Date: 2026-06-25
- Component: `org.icij.extract:extract-lib` (with a small `extract-cli` build fix)
- Related notes (internal): extract-indexing-robustness §8 (the full investigation, measurements, and the Strategy-A/B/C comparison); datashare-installer packaging/OS constraints

## 1. Problem

`java-libpst` 0.9.3 (the last Maven Central release, effectively unmaintained) resolves OST-2013 ("64-bit, 4k page", type-36) blocks correctly but does not inflate their per-block zlib "internal compression". On a real 48 GB OST this is the single first-order cause of dropped attachments: ~95% surface as a silent off-by-one `IndexOutOfBoundsException` in `PSTNodeInputStream.read` (the block list holds N blocks `0…N-1` and a read asks for index N), and ~5% as an `Unable to decompress reportedly compressed block` throw. In both classes java-libpst successfully *resolves* the blocks; only its *decompression* is wrong. The attachment bytes are lost upstream of every Tika parser, so nothing hangs or OOMs and no parser-level record exists. A naive in-JVM salvage decorator (treat the IOOBE as clean EOF) was prototyped and rejected: it returns ~99% of the bytes but they are corrupt, not merely truncated, so they are not reliably indexable.

## 2. Decided approach and rationale

Strategy C: a no-fork, pure-JVM reader inside `extract-lib`. Keep `java-libpst` as an unmodified dependency for descriptor/block resolution, and add a reader that inflates the per-block zlib it omits (`java.util.zip.Inflater`). Because the reader is pure JVM it ships in the jar and works on native Windows, macOS, and Linux with zero native dependency — which is the hard constraint (native Windows extraction, no Docker on Windows). This is the true root-cause fix and recovers 100% of the observed loss on the target corpus.

The libpff per-attachment fallback prototype (`LibpffAttachmentRecovery` + `pff_read_attachment.py`) is superseded by C and is removed. The libpff deep-recovery hedge that earlier framing reserved is also dropped from scope: the only libpff we would ever consume is `apt install pff-tools`, which on Ubuntu is the 2018 build, so its "actively-maintained, tracks format drift" premise does not survive — a 2018 binary tracks post-2018 Outlook drift no better than 2017-era java-libpst, and the standing "never build/host libpff" decision means we would never ship the fresh libyal source that *is* maintained. The hedge only ever addressed hypothetical residual; resolution failures caused zero attachment loss on this corpus (they were folder-level and already handled by orphan/alternate-tree recovery). Instead of building uncertain-value insurance on a stale binary, we make residual loss fully accountable in metadata (§7) so that any future hedge is chosen on fresh evidence.

## 3. The seam into java-libpst

The reader needs java-libpst's resolved block coordinates and file handle: `PSTNodeInputStream.indexItems` (`private final LinkedList<OffsetIndexItem>`), `OffsetIndexItem.fileOffset` (`long`) and `OffsetIndexItem.size` (`int`) (both package-private fields), `PSTFile.getOffsetIndexNode(long)` (package-private), `PSTFile.getFileHandle()` (public), `PSTDescriptorItem.offsetIndexIdentifier` (package-private field), and the package-private `PSTNodeInputStream(PSTFile, OffsetIndexItem)` / `(PSTFile, PSTDescriptorItem)` constructors.

Seam decision: place the reader in `package com.pff;` inside `extract-lib`'s own source tree (`extract-lib/src/main/java/com/pff/OstCompressedBlockReader.java`), gaining compile-time-checked package-private access — no reflection, no `setAccessible`, no `--add-opens`/`--add-exports`. Failures surface at compile time, not runtime, and the code survives refactors more safely than reflection.

Feasibility confirmed: `com.pff` arrives as a classpath jar (transitively via `tika-parser-microsoft-module:3.3.0`), i.e. the unnamed module, so a split `com.pff` package across two jars is legal (illegal only under the JPMS module path; Datashare deploys via classpath). The `java-libpst` 0.9.3 jar manifest has no `Sealed` attribute, so classpath split-package will not fail at class load. If a future JPMS/module-path deployment is ever adopted, this seam must be revisited (a reflection fallback would be the escape hatch) — out of scope here.

## 4. Components

### 4.1 `com.pff.OstCompressedBlockReader` (new)

Two layers, so the decode logic is testable without a real OST.

Pure core (no java-libpst types; the unit-tested unit):

`Optional<byte[]> recover(RandomAccessFile fileHandle, List<Block> blocks, int encryptionType, long declaredSize)` where `Block` is a small value type `(long fileOffset, int size)`. For each block, in order: seek to `fileOffset` and read `size` bytes; if `encryptionType != ENCRYPTION_TYPE_NONE`, replay java-libpst's OST block decode (the compressible-encryption permutation) over those bytes; sniff a valid zlib header (first byte CM nibble `== 8` and the two-byte big-endian `CMF/FLG` value `% 31 == 0`); if it looks like zlib, trial-inflate with an `Inflater` and use the inflated output, otherwise (or if inflation fails) use the raw bytes; append to the output. After all blocks, apply the strict gate: return the assembled bytes only if their total length equals `declaredSize`, otherwise return `Optional.empty()`.

Seam adapter (java-libpst-facing):

`Optional<byte[]> recover(PSTAttachment attachment)`. Resolves the attachment's data-node block coordinates, encryption type (`PSTFile.getEncryptionType()`), and declared size, then calls the pure core. It handles both loss classes:

- Silent class (~95%): `getFileInputStream()` returns a `PSTNodeInputStream` (resolution succeeded); read its package-private `indexItems` to obtain the `(fileOffset, size)` list directly.
- Throw class (~5%): `getFileInputStream()` throws during construction; fall back to descriptor-level resolution — from the attachment's data `PSTDescriptorItem` take `offsetIndexIdentifier`, call `PSTFile.getOffsetIndexNode(...)` to get the `OffsetIndexItem`(s), and read their `(fileOffset, size)`.

The adapter never throws: any failure (resolution, I/O, linkage) returns `Optional.empty()`.

The reader replicates java-libpst's block boundary / trailer handling so that, for the silent class, its output is byte-identical to the bytes libpff recovers (already validated for the silent class in the prototype: reusing the resolved `indexItems` + file handle, reading each raw block, and inflating the `78 9c` blocks reproduces libpff's bytes exactly — JPEGs decode, PDFs parse). The byte-identity contract is enforced by milestone M1 (§9) for the throw class before the rest is built around it.

### 4.2 `ResilientOutlookPSTParser` (changed)

`recoverAndEmitAttachment(...)` calls `OstCompressedBlockReader.recover(attachment)` instead of `LibpffAttachmentRecovery.recoverToTempFile(...)`. Detection (`isUnreadableByValueAttachment`, `countUnreadableAttachments`) is unchanged — it already flags both classes (the truncated-`length()` case for the silent class, the `getFileInputStream()` throw for the throw class) and is already gated to OST-2013 (`PST_TYPE_2013_UNICODE`). Recovered bytes are emitted as a sibling embedded document through the existing `parseEmbedded` path, carrying the same folder path and resource (attachment) name as today, via an in-memory `TikaInputStream` (`TikaInputStream.get(byte[])`) rather than a temp file — the recovered attachment is already fully in memory and the byte arrays are bounded by the attachment's declared size, so a temp file adds I/O and cleanup with no benefit.

### 4.3 Removed

`extract-lib/src/main/java/org/icij/extract/parser/LibpffAttachmentRecovery.java`; `extract-lib/src/main/resources/org/icij/extract/parser/pff_read_attachment.py` (and the now-empty `resources/org/...` tree if nothing else uses it); any libpff / pypff / pst-utils install or documentation steps that the prototype introduced.

## 5. Data flow

The folder walk and message emission are unchanged. For OST-2013 files only, after a message emits, each by-value attachment runs through:

1. `isUnreadableByValueAttachment` — no → healthy, parsed normally by `PSTMailItemParser` (fast path, untouched).
2. yes (truncated `length()` or `getFileInputStream()` threw) → increment `unreadable`; call `OstCompressedBlockReader.recover(attachment)`.
3. `Optional.empty()` (failed the size gate or unresolved) → increment `unrecovered`; the attachment stays an honest, counted loss — nothing is indexed.
4. bytes (total == declared) → `parseEmbedded(bytes)`:
   - parses cleanly → increment `recovered`.
   - throws `EncryptedDocumentException` → increment `recovered` and `encrypted`; the bytes are indexed as an attachment, the body text is not (deferred class, §8).

The healthy path never touches the reader; the reader runs only for already-detected unreadable attachments on type-36 files.

## 6. Error handling and isolation

Every reader call is best-effort and never propagates into the walk, matching the parser's existing `catch (Exception | LinkageError) → log + count as loss` isolation pattern used throughout `ResilientOutlookPSTParser`. The strict size gate (§4.1) is the correctness backstop and the direct encoding of the rejected-salvage lesson: bytes that cannot be fully accounted for against the declared size are dropped and counted as `unrecovered`, never emitted. There is no temp-file lifecycle to leak (emission is from an in-memory stream).

## 7. Metadata and accountability

Set on the root document, OST-2013 only (the fields stay absent on every other PST so they read as "not applicable" rather than a misleading "0"):

- `tika:pst_attachments_unreadable` (kept) — by-value attachments detected as unreadable by java-libpst.
- `tika:pst_attachments_recovered` (kept; semantics updated) — bytes recovered and emitted by the in-JVM zlib reader (was "via libpff").
- `tika:pst_attachments_unrecovered` (new) — detected-unreadable attachments the reader could not recover (failed the size gate or unresolved); the honest residual.
- `tika:pst_attachments_encrypted` (new) — recovered attachments whose bytes parse as password-protected; a subset of `recovered`, surfaced so the deferred class is visible.
- `tika:pst_expected` / `tika:pst_emitted` / `tika:pst_failed` (kept as-is) — message-level reconciliation; this is the accountability that would let a future deep-recovery hedge be justified on measured evidence rather than assumption.

Invariant: `unreadable == recovered + unrecovered`. `encrypted <= recovered`.

## 8. Explicitly deferred — encrypted attachments

Password-protected attachments (e.g. some PDFs on the big OST) are a distinct residual class needing credentials, not a reader fix. The reader recovers their bytes perfectly and emits them (so they are indexed as attachments), but Tika cannot extract body text and throws `EncryptedDocumentException` at parse. We count them (`tika:pst_attachments_encrypted`) and document the limitation. No password/credential infrastructure is built.

## 9. Testing

TDD throughout. Order chosen to de-risk the one unproven path first.

Unit tests (CI, no real OST):

The pure core (§4.1) against synthetic temp files with hand-built block layouts — single raw block; single `78 9c` zlib block; multi-block all-raw; multi-block raw-then-zlib (the `1.jpg` shape: first blocks uncompressed, later blocks zlib); a compressible-encrypted block (verifying the decode replay); size-mismatch input → `Optional.empty()` (the gate); a block whose first two bytes coincidentally satisfy the zlib header check but do not inflate → raw fallback. These cover the decode/inflate/reassemble/gate logic deterministically without any java-libpst dependency.

Integration test (end-to-end):

Obtain or craft a minimal OST-2013 (type-36) fixture that reproduces the multi-block zlib defect, committed to `extract-lib/src/test/resources/documents/pst/` for a real `ResilientOutlookPSTParser` end-to-end test asserting unreadable→recovered counts and that a recovered JPEG decodes and a recovered PDF parses. The 66 MB local OST is too large and contains confidential data, so the fixture must be minimized + scrubbed or hand-built. Fallback if no committable fixture proves achievable: an opt-in test gated on `-Dost.test.file=<path>` that runs against the local OSTs and is skipped in CI when the property is absent.

Regression: the existing `ResilientOutlookPSTParserTest` over `testPST.pst` must stay green — non-OST-2013 PSTs never enter the reader path and pay nothing.

Before/after validation (manual, local): run the CLI (made runnable by §10) over `/path/to/big.ost` (48 GB) and the 66 MB OST with OCR and 6 GB heap, and compare `unreadable` / `recovered` / `unrecovered` counts against the pre-change baseline in the KB (271 IOOBE + 7 decompress + 16 truncated on the big OST). Goal is recovery counts, not collecting files.

## 10. CLI validation-enablement (`extract-cli`)

Running a real OST for before/after counts needs a runnable CLI fat jar; two harness defects (KB §8) block this:

- `commons-io` pin: pin `commons-io 2.21.0` in `extract-cli/pom.xml` (already staged in the working tree) so the fat jar's Tika 3.3.0 resolves `UnsynchronizedByteArrayInputStream.builder()` instead of the 2.8.0 that `solr-solrj` pulls transitively and wins mediation with.
- parser service-file clobber: the `maven-assembly` `jar-with-dependencies` overwrites Tika's `META-INF/services/org.apache.tika.parser.Parser` with extract-lib's one-line file, which references a non-existent class (`org.icij.extract.ocr.Tess4JOCRParserTest`), leaving `DefaultParser` with 0 parsers so every file falls through to `ErrorParser`. Fix the service-file handling (merge, or correct/remove the dangling entry) so the fat jar parses normally. Datashare's separate-jar classpath unions the service files and is unaffected, which is why this never surfaced in production.

This section is scoped narrowly to making the CLI usable for OST triage/validation; it is not a broader build cleanup.

## 11. Milestones (implementation order)

- M1 — Prove the throw-class (~5%) seam. Implement only the throw-class descriptor-level resolution (`offsetIndexIdentifier` → `getOffsetIndexNode` → raw block read → inflate) and match its output byte-for-byte against the libpff truth files already captured in scratchpad, so "C subsumes libpff for the zlib classes" is measured, not argued. Gates building the adapter around the throw-class path. (The silent class is already prototype-proven.)
- M2 — Pure core (§4.1) + unit tests, red→green. Runs in parallel with M1 (it is shared by both classes and has no java-libpst dependency).
- M3 — Seam adapter (§4.1) wiring both classes onto the pure core.
- M4 — Rewire `ResilientOutlookPSTParser` (§4.2); remove the libpff path (§4.3); add metadata (§7).
- M5 — CLI fix (§10); fixture + integration test (§9); before/after validation on the small + big OST.

## 12. Out of scope

- The libpff (or any native) deep-recovery hedge — dropped (§2); revisit only if reconciliation metadata on a future corpus shows material resolution loss.
- Decrypting password-protected attachments (§8).
- The broader process-isolation / ledger work in KB §1–§7 (orthogonal; this spec is the reader fix only).
- Any JPMS module-path deployment of extract-lib (would invalidate the split-package seam, §3).
