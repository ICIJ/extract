# Code review (max): OCR automatic language selection (master..feat/ocr-auto-language)

Reviewed after the fixes from `2026-10-01-ocr-auto-language-final-review.md` (probe memoized per JVM, zero-word retry skipped, plain eng offsets pinned). Findings are ordered by severity. Each one was reproduced or traced to source by at least one reviewer; agreement across reviewers is noted.

## High

### 1. Auto mode never turns on with stock Debian/Ubuntu script packages
- Where: `extract-lib/src/main/java/org/icij/extract/ocr/AutoLanguageOCRParser.java:45-60`, `Extractor.java:406-416`, `.circleci/config.yml:41-44`
- What: `tesseract-ocr-script-latn` installs `tessdata/Latin.traineddata` at the root (checked: `dpkg -L` here prints `/usr/share/tesseract-ocr/5/tessdata/Latin.traineddata`), so `--list-langs` reports `Latin`, not `script/Latin`. The probe reports all six script models missing and disables routing. The `script/` folder in this container and in CI is hand-made symlinks no package owns. Tika's language regex (`TesseractOCRConfig.java:124-125`) rejects bare `Latin`, so a real `script/` directory is needed.
- Impact: ICIJ's `datashare-extract` image (Ubuntu noble, `tesseract-ocr-all`) logs "missing tesseract models" for installed packages and OCRs everything as `eng`. The feature only works in CI and in this container. (4 reviewers)
- Fix options: resolve each model as `script/X` or `X` from `installedModels()` and pass the right form to Tika (needs a check that Tika accepts it), or document and ship the symlink step in the Docker images.

### 2. OSD pass leaks an empty `.osd` temp file per text-poor image (TESSERACT)
- Where: `AutoLanguageOCRParser.java:123-134`
- What: on images with too few characters, `tesseract --psm 0` writes an empty `.osd` and exits 1. Tika throws before it records the output file (`TesseractOCRParser.java:334-341`), so its `finally` deletes nothing. `detect()` swallows at DEBUG.
- Impact: one orphan file per logo, photo, signature or blank scan, forever. `/tmp` here holds 214 of them from today's test runs. A million-image run means a million files. (4 reviewers)
- Fix: give the OSD pass its own temp dir via `TemporaryResources`/a scratch directory and delete it in `finally`, or delete `<base>.osd` after the call.

### 3. Default extraction text changed, so Datashare page offsets drift from indexed content
- Where: `Extractor.java:284-297`, `HocrTextHandler.java:71-82`, `AutoLanguageOCRParser.java:151,162-169`
- What: with no `ocrLanguage`, extraction now depends on which models the host has. The hOCR rebuild (and Tess4J `.strip()`) drops tesseract's trailing newline, so each OCR'd image loses 1 char even with identical recognized text. Reproduced: `embedded.pdf` gives `[(0,15),(16,31)]` with models and `[(0,16),(17,33)]` without.
- Impact: Datashare re-runs OCR in `getPages` (`DocumentResource.java:182`) and `PageArtifact.java:136`, then slices the stored `content`. The client's only drift guard compares the Tika version, which is unchanged. Workers (`tesseract-ocr-all`) and web host (`tesseract-ocr`) can disagree, and every pre-upgrade document drifts once models are present. `StructureMarkdownExtractor` rebuilds OCR with stock eng, so it no longer matches indexed text either. (5 reviewers)
- Fix: end the last line like tesseract's text output and drop the strip, so Latin images keep master's layout. The script-dependent part needs a Datashare decision (pin `ocrLanguage` on the web side, or record `ocr:model` per document and replay it).

## Medium

### 4. Images the delegate would skip now get OCR markup and metadata
- Where: `AutoLanguageOCRParser.java:88-117`
- What: the router never checks `skipOcr`, min/max file size, or a caller's psm 0. It always spools, calls the delegate, emits `<div class="ocr">`, `ocr:model=script/Latin` and `ocr:confidence=0.0`. Master emitted nothing in those cases (`TesseractOCRParser.java:254-256`, `Tess4JOCRParser.java:378-383`). Reachable via TESS4J `setSkipOcr(true)` (its `getSupportedTypes` ignores it) or `maxFileSizeToOcr`.

### 5. Failures before the first read skip `OCRParserAdapter`'s content-type restore
- Where: `AutoLanguageOCRParser.java:95`
- What: spooling moved out of the adapter's try/finally. An IOException there (full tmp, made likelier by #2) leaves `Content-Type: image/ocr-png` on the indexed embed, the exact case `OCRParserAdapter.java:47-55` guards. (2 reviewers)
- Fix: restore the content type in the router's `finally`, or spool inside the first delegate call.

### 6. Forced hOCR returns empty text when `tessdata/configs/hocr` is missing
- Where: `AutoLanguageOCRParser.java:141-142`
- What: reproduced with tesseract 5.3.4: a `TESSDATA_PREFIX` with all 8 models but no `configs/` passes the probe; `tesseract ... hocr` prints `Can't open hocr`, exits 0, writes only `.txt`. Tika finds no `.hocr` and emits nothing. Empty text also skips the retry. Master's txt mode works on the same folder.
- Fix: add `configs/hocr` to the probe, or fall back to plain text when the hOCR read yields nothing.

### 7. `ocrTimeout` now bounds each pass, not each image; Tess4J never cancels timed-out jobs
- Where: `AutoLanguageOCRParser.java:96-109`, `Tess4JOCRParser.java:84,666-678`
- What: up to three runs per image each get the full timeout (`setOcrTimeout` doc at `Extractor.java:566` says "no longer than the given duration"). On TESS4J, a timed-out retry keeps running on the single executor thread, later writes `ocr:confidence` into the live `Metadata` (possible `ConcurrentModificationException` against the spewer), and every queued pass times out behind it. `test_caller_settings_reach_every_pass` pins `List.of(42, 42, 42)`. (3 reviewers)
- Fix: `future.cancel(true)` on timeout; pass a scratch `Metadata` to the retry and copy only on win.

### 8. extract-cli never installs the router unless `--ocrType` is given
- Where: `Extractor.java:292-297`
- What: the CLI pre-registers every option empty (`CommonsTransformer.java:32`), and `Options.get(name, default)` returns the empty option (`Options.java:25-27`). `setOcrConfig` is skipped, OCR stays on stock eng, and `--ocrRetryConfidence` is ignored, contradicting the new help text. Datashare is unaffected (it always passes `ocrType`). (3 reviewers)

### 9. Retry policy costs 9 to 14x master's CPU on degraded Latin scans
- Where: `AutoLanguageOCRParser.java:98,100`
- What: measured on 8 degraded copies of an English page: 5 retried, the retry alone taking 17 to 41 s CPU vs 2.2 to 3.7 s for master's single eng read, and it always "won" while adding non-Latin letters. Unmapped OSD scripts (OSD said "Bengali" on a degraded English page) send the first pass straight to the 6-model read. Each 6-model tesseract process peaks at about 360 MB vs about 100 MB for eng, so at `ocrParallelism=16` that is about 5.9 GB off-heap.
- Note: threshold and mean-confidence selection are spec decisions; this is measured evidence for revisiting them, not a code bug. Cheap mitigations: treat unmapped scripts as Latin, cap concurrent retries.

### 10. A failed probe is cached for the JVM lifetime
- Where: `Extractor.java:190,406-416`, `TesseractOCRConfigAdapter.java:52-56`
- What: Tika swallows `--list-langs` IOException, interrupt and timeout into an empty set (`TesseractOCRParser.java:895-898`), which the cache stores as "all missing". Reproduced: one interrupt during the probe leaves every later Extractor silently on eng with no warning. Same when `eng.traineddata` is absent: `initialize()` throws on the default language after listing correctly, and the adapter returns `Set.of()`. Subprocesses also run inside `computeIfAbsent`, blocking other constructions. (3 reviewers)
- Fix: don't cache an empty result; in the adapter, return `probe.getLangs()` from the catch when non-empty.

## Low

### 11. Tess4J multi-page TIFF confidence is the last page's only
- Where: `Tess4JOCRParser.java:646-653`
- What: `TessBaseAPIMeanTextConf` after `ProcessPages` scores the last page. A TIFF with a blank last page scores 0, always retries all pages with 6 models, and records `ocr:confidence=0.0`. The CLI backend averages all words, so backends disagree on the same file. `createDocumentsWithResults` also recognizes every page twice to build an unused word list. (3 reviewers)

### 12. Eager `orElse(tesseract.getOSD(image))` now runs on every TIFF
- Where: `Tess4JOCRParser.java:572-574` (and `:595-596`)
- What: pre-existing, but OSD now runs on every image. TIFFs run detection twice, `getOSD(File)` leaks a `FileInputStream`, and a null `BufferedImage` throws an NPE. Fix: `orElseGet`. Listed in the final review's "fix now", still not done. (5 reviewers)

### 13. One blank page throws away detection for a whole multi-page TIFF
- Where: `AutoLanguageOCRParser.java:130-133`
- What: Cyrillic page 1 plus blank page 2: tesseract exits 1, Tika discards the `.osd`, the doc is read as Latin (measured 47.8) and retried over every page.

### 14. `OCRConfigAdapter.installedModels()` is abstract on a published interface
- Where: `OCRConfigAdapter.java:24`
- What: reproduced `AbstractMethodError` from `setOcrConfig` with an adapter compiled against master. No implementer found outside this repo. Fix: `default Set<String> installedModels() { return Set.of(); }` (routing then turns itself off with the existing warning).

### 15. New `ocr:*` metadata never reaches PDF documents
- Where: `AutoLanguageOCRParser.java:110-117`, `EmbedSpawner.java:376-379`
- What: only `OCR_PARSER` is copied to the parent for inline images, and rendered pages use a discarded `renderMetadata`. Scanned PDFs, the main OCR input, never record the model. Also: `emit()` runs before the fields are set, so HTML output heads never carry them. (Final review minor 3, still open.)

### 16. Default-config tests silently require osd plus six `script/` models
- Where: `ExtractorTest.java:573,655,669,684`, `embedded-data-uri-pdf.html`
- What: no Assume guard; on a plain `tesseract-ocr` host they fail with the eng offsets. `README.md:72` still says only Tesseract and Leptonica are needed.

## Simplifications worth taking

- `AutoLanguageOCRParser.java:171-179`: `copyOf` duplicates `ParserUtils.cloneMetadata` (tika-core 3.3.0).
- `AutoLanguageOCRParser.java:46-60`: the six script models are typed in `ALL_SCRIPTS`, `REQUIRED_MODELS` and `FIRST_PASS_MODEL`; derive the first two from one list so the probe can't drift from what the router asks for.
- `Tess4JOCRParser.java:119-124`: OSD metadata keys redefined as literals while the router reads Tika's `PSM0_SCRIPT*`; alias the Tika constants so a rename can't silently route everything to Latin.
- `AutoLanguageOCRParser.java:39`: extend Tika's `ParserDecorator` instead of hand-delegating. Also restores `X-TIKA:Parsed-By` and lets `replaceParser` see through wrappers (the `ocrCache` option has matched nothing since `OCRParserAdapter`, pre-existing).

## Still open from the final review

- `detect()` catches only checked exceptions (`AutoLanguageOCRParser.java:130`). Verified unreachable on tesseract 5, reachable on 4.0 and older (`Script confidence: inf` makes `Double.parseDouble` throw).
- `orElseGet` (finding 12).
- Unused `Metadata` import in `ExtractorAutoLanguageTest.java:7`.
- PR description: Tess4J empty-text fix and the whitespace/offset change.
- Commit `59763fb` has a `Co-Authored-By: Claude` trailer; reword before the first push.
