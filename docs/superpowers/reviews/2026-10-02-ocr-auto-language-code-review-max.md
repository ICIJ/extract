# OCR auto-language: code review (max)

Branch `feat/ocr-auto-language` vs `master`. Ten finders plus one verifier. "Repro" means a finder ran the code in the container (tesseract 5.3.4, Tika 3.3.0). Nitpicks skipped. Conventions finder: no CLAUDE.md violations.

## Critical: the feature silently does not run, or regresses defaults

1. **Routing never turns on with Debian/Ubuntu packaged models.** `AutoLanguageOCRParser.java:50-52` requires `script/Latin` etc., but `tesseract-ocr-script-*` (and `tesseract-ocr-all`, recommended by the Datashare .deb) install `Latin.traineddata` at the tessdata root. `missingModels` is never empty, a WARN lists installed models as missing, OCR stays on eng. Only CI (`.circleci/config.yml:43` symlinks) and this container have a `script/` dir. Tika rejects bare `Latin`, so the fix is the install layout (shared install step, docs) or a probe that recognises root names and says which links to create. Verified (dpkg -L, end-to-end run on a stock tessdata). Homebrew/tessdata_fast is fine.
2. **CLI OCR timeout drops from 1 day to 120 s.** `Extractor.java:268-270` sets eng + 1 day, but `configure` now always calls `setOcrConfig` (`:294`) with a fresh adapter, and a CLI-registered `ocrTimeout` with no value parses to empty, so `:300` sets nothing. `extract spew` without `--ocrTimeout` gets Tika's 120 s (`TesseractOCRConfig.java:95`). Large scans now time out. Repro.
3. **`setOcrLanguage()` / `setOcrConfig()` after construction are ignored.** `Extractor.java:563-565` only clears `autoOcrLanguage`, read only in `withAutoLanguage` (`:404`), which already ran in the constructor. The router overwrites the language every pass (`AutoLanguageOCRParser.java:184-185`). `new Extractor(); setOcrLanguage("rus")` reads with `script/Latin`; master used rus. `setOcrConfig` cannot evict the router either (`replaceParser` matches the raw parser class, `:1077`). `ExtractorTest.testExtractorShouldSupportMultipleLanguage` still passes and hides it. Repro (5 finders).
4. **Retry degrades low-confidence Latin pages.** `AutoLanguageOCRParser.java:122-126`: the six-script pass outscored `script/Latin` on all 14 sampled pages, so under 60 it always wins. Degraded English scans gain Cyrillic look-alikes and altered numbers ("250,000" -> "350,000", "was" -> "маз"). It can also win with far less text (1489 -> 467 chars). Repro.

## High: wrong or lost output

5. **Bad `ocrScriptLanguages` values fail per image, not at startup.** `AutoLanguageOCRParser.java:84-96` checks only shape and installed models. `Cyrillic:rus+`, `Latin:deu+Fraktur`, `Cyrillic:Cyrillic` pass, then `config.setLanguage` (`:185`) throws `IllegalArgumentException` outside any catch, and every matching image loses its text. A bad `Latin:` value also hits every failed-detection image. Repro.
6. **Datashare page offsets shift for already-indexed documents.** `Extractor.java:289-290` drops the implicit eng. Datashare re-extracts to build page indices (`DocumentResource.java:182`, `PageArtifact.java:136`) against content indexed with eng. `test.png` is 420 bytes with eng vs 425 with `script/Latin`, so every later page boundary moves; the trailing-newline fix (`AutoLanguageOCRParser.java:199-203`) only helps when both models agree. Repro.
7. **Datashare structure pages no longer match content.** `StructureMarkdownExtractor` runs stock Tika OCR with eng; content now goes through the router. A Cyrillic image is indexed correctly while its structure page is Latin garbage. Repro.
8. **CLI hOCR path changes text output.** `AutoLanguageOCRParser.java:187-191` forces hOCR and rebuilds text: multi-page images gain a blank line between pages, and `pageSeparator`, `preserveInterwordSpacing`, `outputType=HOCR` no longer apply. Repro.
9. **Tess4J with rotation/preprocessing deletes the router's spooled image.** `Tess4JOCRParser.java:435-446` writes to and deletes `pathWithExt`, which is the router's temp file when the suffix matches the MIME extension. The OSD pass deletes it, the read fails with `NoSuchFileException`. Master ran one pass. Repro.
10. **Multi-page images are judged on their last page.** OSD keeps only the last page's script (`AutoLanguageOCRParser.java:117`), and one model reads every page: a Cyrillic page followed by a Latin page reads page 1 as `Topogckoň coBeT...` at confidence 0.74, so no retry. Repro. On Tess4J, confidence is also last page only: multi-page TIFF reports 0.59 vs 0.77 on the CLI, so only Tess4J retries, and a garbled first page with a clean last page never retries.
11. **`ocrScriptLanguages` keyed by model name is silently ignored.** OSD reports Hangul as `Korean`, so `Hangul:kor` is never used, with no warning. Repro.

## Medium: operations, cost, logs

12. **No tesseract on the host = a stack trace per Extractor.** `Extractor.java:407-414` never caches an empty probe; `TesseractOCRConfigAdapter.java:50-53` forces `preloadLangs`, which Tika runs even without the binary and WARNs with a stack trace. Datashare builds one Extractor per document and per pages request. Tess4J without osd prints native errors, about 120 ms each. Master logged nothing. `computeIfAbsent` fixes it in one line and deletes the special-case test. Repro.
13. **Timeouts multiply.** A timed-out OSD (`:158`) is swallowed at DEBUG, then each read gets the full `ocrTimeout` again: 2-3x the configured bound per image. Tess4J timeouts are not cancelled (`Tess4JOCRParser.java:664-673`), so the next pass queues behind the hung one.
14. **Up to 3 tesseract runs and 4-5x memory per image.** Peak RSS 71 MB (eng) -> 271-357 MB, times `ocrParallelism`. The six-model retry was 32% of OCR time on a 70-image sample; 7 of 13 retries were on near-empty garbage (skip the retry under about 50 chars). Scripts outside the six (Fraktur, Greek) go straight to the six-model read: 2.8-4.8x slower and less text than `script/Latin`.
15. **Tess4J runs every pass on one thread.** `Tess4JOCRParser.java:85` single-thread executor now serialises 2-3 passes per image across all OCR threads; 8 threads were 3.6-4.8x faster with the same output.
16. **Multi-page TIFF runs OSD on every page** but Tika keeps only the last page's script; a text-poor page fails the whole detection. Detect on frame 0.
17. **JVM caches never refresh and warn once per adapter class.** `Extractor.java:193-194, 416`: models installed while Datashare runs are ignored until restart, and later, different missing models are never logged.
18. **Orphan cleanup is narrow and expensive.** `AutoLanguageOCRParser.java:168-179` lists all of `java.io.tmpdir` on every failed OSD (O(N²) in archives with deferred images), stops at the first undeletable file, and only handles `.osd`: failed reads leave `.hocr` and `.txt` too. A single cleanup in `OCRParserAdapter`'s finally would cover all of them. Repro.
19. **`--ocrCache` never caches the main OCR path** (pre-existing, `Extractor.java:318`). Now costs 2-3 passes per uncached image. A naive fix would drop `ocr:model`/`ocr:script` on cache hits.

## Low

20. **`test_failed_detection_leaves_no_osd_file_behind` is flaky.** It counts `.osd` files in shared tmp while the cleanup deletes everyone's orphans. Reuse `ExtractorTest.tikaTempFiles()` with its before/after set difference.
21. **Inverted `datapath()`** (pre-existing, `Tess4JOCRParser.java:354-362`): a set `tessdataPath` is replaced by the libtesseract path; the new probe inherits it. Not reachable today.

## Simplifications

- `Extractor.java:268-270` is dead code (see 2); `autoOcrLanguage = false` in `setOcrLanguage` and the `= true` initializer never change anything.
- `Extractor.java:290` and `:314` parse the `ocr` option twice with two different defaults; `:416-421` tests `!missing.isEmpty()` twice.
- `AutoLanguageOCRParser.java:119-120` spells the failed-detection fallback twice; normalise `script` to `"Latin"` once.
- `OCR_SCRIPT` / `OCR_SCRIPT_CONFIDENCE` duplicate Tika's `tess:script` fields (keep only if the `ocr:` names are intentional).
- `OCRConfigAdapter.installedModels()` default is dead; make it abstract.
- Tests: `squash()` re-implements `StringUtils.deleteWhitespace`; two test pairs share Given/When; new `parseWith` bypasses the class's resource helper.
- Tess4J OSD loads eng first (about 25 ms/image): set the detection language to `osd`.
