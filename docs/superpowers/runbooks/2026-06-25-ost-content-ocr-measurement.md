# Runbook — OST-2013 content/OCR measurement pass (F5)

Status: to execute once a 48 GB OST and an OCR environment with eng+<lang>
tessdata are available. The big-OST validation run used a counting extractor, so the content
and OCR layer was never exercised; this run surfaces that layer's error profile.

## Prerequisites
- The 48 GB OST file on local disk (java-libpst reads on demand; ~570 MB RSS observed).
- Tesseract with `eng`, `osd`, and `<lang>` tessdata installed (<lang> closes the recall gap; see ocr-language-packages). <lang> install is a datashare-installer hand-off.
- extract built from branch `feat/ost-2013-attachment-recovery`.

## Invocation
- Real `Extractor` → SHA-256 → `ResilientOutlookPSTParser`, `embedHandling=spawn`, OCR `AUTO`, languages `eng+<lang>`, ergonomic heap, output to a real spewer (or `/dev/null` sink for insight-only).
- This differs from the prior run by using the real Tika parse + OCR + spewer rather than the counting extractor.

## Capture (per signal)
1. PDFBox parse-failure profile: counts by exception class (e.g. TIKA-198, encryption), distinct from the font-noise demoted in Task 4. Confirm Task 4's logger names actually silence `No current font` in this build; add any missed name.
2. OCR recall/quality on recovered JPEGs/PDFs with <lang> installed vs eng-only.
3. Encrypted-attachment behaviour: `pst_attachments_encrypted` (note the Tika-swallow caveat — it may read 0; cross-check by counting EncryptedDocumentException at the parse layer if a custom extractor is used).
4. Reconciliation: `pst_expected` / `pst_emitted` / `pst_failed` and `pst_attachments_unreadable/recovered/unrecovered/encrypted`. Expect parity with the prior counting run (~12,959 unreadable → ~11,547 recovered, 0 net message loss) since F3 is attribution-only.
5. Folder attribution: count of messages stamped `/[recovered]` — expect it to drop versus the pre-F3 baseline (F3 now attributes most recovered messages to their real folder).

## Pass/fail
- No new net message loss (`pst_expected == pst_emitted`).
- Font-noise no longer dominates WARN volume (Task 4 effective).
- <lang> materially improves OCR text on the non-English scans.
- `/[recovered]` attribution count strictly lower than the pre-F3 run.

## Hand-offs surfaced (not extract-lib code)
- datashare: index/expose `pst_*` metadata so reconciliation and the encrypted residual are queryable and alertable (F3 second half).
- datashare runtime logback: mirror Task 4's PDFBox logger demotion.
- datashare-installer: install `<lang>` tessdata per channel (apt / brew / snap stage-packages).
