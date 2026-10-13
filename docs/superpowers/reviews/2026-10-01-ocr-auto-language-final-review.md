# Final review: OCR automatic language selection (04d4916..58f9a89)

### Strengths

- The router is a thin decorator. Every native call goes through the existing `OCRParserAdapter`, so media-type restoration, `OCR_PARSER` and the deferred-OCR path keep working unchanged. I checked that `AbstractPDF2XHTML` page OCR (`EmbeddedDocumentUtil.getStatelessParser`) and `ImageIOTranscodingOCRParser` both reach the router.
- The per-pass configs are clones (`SerializationUtils.clone`) and the caller config goes back into the context in a `finally`. The shared `Extractor` `TesseractOCRConfig` is never mutated, which is safe on the multi-threaded fan-out pool.
- Tess4J: dropping the shared `volatile tesseract` field fixes a real race (concurrent parses overwrote it). I verified deviation 4 against the Tess4J 5.15 bytecode: `createDocumentsWithResults`, `getOSD` and `doOCR` all call `init()`/`dispose()` per call, so a per-model cache would save nothing.
- The pass-2 failure ruling is right: losing a good pass 1 because the 6-model pass failed would turn images that extract today into failures.
- Focus items 1 to 5 are pinned by real tests. I re-ran `ExtractorAutoLanguageTest` (exit 0). `simple.tiff` on `TESSERACT` logs no per-image WARN. The only WARNs were the expected startup and clamping ones.
- Startup check is all-or-nothing, as the spec says. `ocr=false` skips the probe entirely (`Extractor.java:283`).

### Issues

#### Critical (Must Fix)

None.

#### Important (Should Fix)

1. **One WARN and one model probe per `Extractor`, and Datashare builds one `Extractor` per document.** `extract-lib/src/main/java/org/icij/extract/extractor/Extractor.java:400-412`.
   - What's wrong: `withAutoLanguage` runs the probe and logs the WARN on every construction, because `ocrLanguage` is absent by default.
   - Where Datashare builds Extractors: `datashare-index/.../PageArtifact.java:136` makes one per document ("One Extractor per document"), `datashare-app/.../DocumentResource.java:475` makes one per request, and `IndexTask.java:178` makes one per task. Datashare has no `ocrLanguage` default (`DatashareCliOptions.java:643`).
   - Why it matters: Datashare images ship without the script models today, so every install logs `OCR language detection disabled, missing tesseract models [...]` once per paginated document and once per source/pages request. Across a million-document artifact run that is a million WARN lines. The spec promised "one warning at startup". Each construction also spawns `tesseract --list-langs` (about 5 ms) or, on TESS4J, loads `osd.traineddata` through `TessBaseAPIInit3`. That cost is small but paid on every request.
   - Fix: memoize `installedModels()` per adapter class in a static field (JVM lifetime), and log the WARN once per JVM with a static `AtomicBoolean`. About 15 minutes, plus one test asserting that two Extractors log one WARN.

2. **Images with no text always pay three passes, including the 6-model pass.** `extract-lib/src/main/java/org/icij/extract/ocr/AutoLanguageOCRParser.java:100`.
   - What's wrong: zero words means mean confidence 0, which is below 60, so the router retries with the combined set. The plan pins this in `AutoLanguageOCRParserTest.java:163` (`osd, script/Latin, ALL`).
   - Measured locally with tesseract 5 (hOCR):

     | Image | Today (`eng`) | Auto mode (osd + Latin + combined) |
     |---|---|---|
     | blank 600x400 | 51 ms | 37 + 73 + 304 = 414 ms (8x) |
     | `simple.tiff` | 56 ms | 29 + 87 + 308 ms |

   - Why it matters: embedded photos, logos, signatures and blank scans are the bulk of OCR inputs in email and PDF leak datasets, so auto mode multiplies OCR time on its most common input. The spec only promises "about one detection pass more" for Latin images. It says nothing about text-less ones, but a Datashare operator would expect the same order of cost. This is a plan issue: the plan chose and pinned this behavior.
   - Fix: skip pass 2 when pass 1 produced no words. Tesseract's layout analysis finds text lines before recognition and does not depend on the model, so a pass that finds no lines with `script/Latin` finds none with the combined set either. Record word count in `Pass` (HocrTextHandler already counts `confidenceCount`; TESS4J can use `text().isEmpty()`) and flip the test to expect `osd, script/Latin`. About 15 minutes.
   - Related, not fixed by this: low-confidence Latin scans (faxes, photocopies) also always pay the combined pass. That one is spec decision 1/3, so measure it on a real dataset before changing the default.

3. **The explicit-language TESS4J path changed, but the PR notes don't say so.** `extract-lib/src/main/java/org/icij/extract/ocr/Tess4JOCRParser.java:466` and `:654`.
   - What's wrong: before c83af83, TESS4J with the default `skipConfidence=false` read `<base>` instead of `<base>.txt`, so it produced empty text. The Task 2 RED run shows `<'\n'>` for `test.png`. It also ignored per-call language and PSM after the first parse.
   - Why it matters: TESS4J users with `ocrLanguage` set now get text, which breaks the Global Constraint "explicit ocrLanguage behaves exactly as today" as written. It is a good change, but downstream users will see newly indexed text and `ocr:confidence` on TESS4J documents.
   - Fix: list it in the PR as `fix: Tess4J returned empty text unless skipConfidence was set`. Doc only, 2 minutes.

#### Minor (Nice to Have)

1. `AutoLanguageOCRParser.java:130`: `detect()` catches only checked exceptions, so a `RuntimeException` during OSD fails an image that OCRs fine today. Catch `RuntimeException` too (one line).
2. `Tess4JOCRParser.java:574` (and `:596`): `orElse(tesseract.getOSD(image))` runs eagerly. This is pre-existing, but now on every auto-mode detection: TIFF or preprocessed images on TESS4J run OSD twice, and a null `BufferedImage` gives an NPE and a per-image WARN. Use `orElseGet`.
3. With PDF `ocrStrategy` not `NO_OCR`, Tika passes the `RenderResult` metadata to the OCR parser (`AbstractPDF2XHTML`). Rendered pages are routed, but `ocr:script/model/confidence` never reach the document.
4. When the tesseract binary is absent, the startup WARN lists all 7 models as missing instead of saying tesseract is missing (`TesseractOCRConfigAdapter.installedModels`).
5. A losing pass (pass 1 or 2) still writes its parser metadata (for example Tess4J `tess:rotation`) into the document metadata, because `read()` uses the real `Metadata`.
6. Unused `Metadata` import in `ExtractorAutoLanguageTest.java:7`.

### Deferred minors triage

None of the deferred minors blocks the merge. Three are worth fixing in the same pass because they are cheap:

- **Fix now:** Task 4 "detect() catches only checked exceptions": a one-line catch widening that prevents a regression against today (Minor 1).
- **Fix now:** Task 5 "plain-eng page offsets no longer pinned": add one `ocrLanguage=eng` page-offset test with the old values (16/33). The no-models path is what every Datashare install runs today, and CI no longer exercises its offsets.
- **Fix now:** Task 5 "unused Metadata import": lint noise.
- **Leave:** Task 1 caption/textfloat tests and the x_wconf word boundary (`title` never has another key ending in `x_wconf`). Task 2: OSD WARN half never RED, empty-script confidence recorded (the router ignores it), `Library.Handler` per parse (JNA caches the library), `isNotNull` confidence checks. Task 3: unavailable-backend test, LinkageError-only catch (JNA load failures are `UnsatisfiedLinkError`). Task 4: stub reads context config, `readsHocr` without a unit test (integration tests cover it), 3x timeout worst case (the default timeout is 12 h). Task 5: probe-skip test, startup-check test without a document.

### Rulings assessment

- **Fresh Tesseract per parse, model listing via `TessBaseAPIGetAvailableLanguagesAsVector`:** sound. Verified against Tess4J bytecode (init/dispose per call). The default datapath `""` makes a directory listing impossible.
- **Branch in the main checkout:** fine.
- **Task 1 null guard:** correct. `Set.of(...).contains(null)` throws.
- **`env -u DISPLAY`:** fine. Test environment only, and CircleCI has no DISPLAY.
- **Task 2 empty-string filter:** correct. It is the only way to honor "failed detection records no script".
- **Task 4 failed retry keeps pass 1:** agree, see Strengths. Cost: a retry failure becomes a WARN line instead of a document error. That is the right trade.
- **Task 5 CI guard:** correct. The trailing `grep -E '^(osd|script/)'` fails the step if no model ends up visible, so a missing apt package will not pass silently.

### Declined to judge

- Datashare page offsets for documents indexed in plain `eng` mode and re-paginated after models are installed: a Datashare follow-up, outside the extract plan.
- OCR cache key not including language or routing mode: pre-existing, plan deviation 7.
- Quality of the 60 threshold and of selection by mean confidence: spec decisions 1 and 3, backed by the spec's measured evidence.
- `datapath()` returning the native library path when `tessdataPath` is set: pre-existing semantics, preserved verbatim.
- `setOcrConfig` re-configuration not replacing an already-installed adapter (`replaceParser` by class): pre-existing.
- `parseTimeout` watchdog interrupts landing inside the swallowed retry: Extractor timeout mechanics are outside the plan, not traced.
- Tess4J's own logger (`net.sourceforge.tess4j`) on an OSD failure: not checked. Only the `Tess4JOCRParser` logger is pinned.
- Windows `script\` model paths: no Windows target in CI or the plan.
- Whether notesalexp ships the `tesseract-ocr-script-*` packages: cannot verify offline. The CI grep fails loudly if not.
- Non-volatile `autoOcrLanguage` read on parse threads: it is written before threads start (thread start and executor submit give happens-before). Only a mid-run `setOcrLanguage` races, and that is not a supported use.

### Recommendations

1. Memoize the model probe and log the WARN once per JVM (Important 1), before any Datashare bump.
2. Skip the retry on zero-word first passes (Important 2), and update the pinning test.
3. Add the TESS4J empty-text fix and the whitespace/offset changes to the PR description.
4. Same pass: widen the `detect()` catch, switch to `orElseGet`, add the plain-`eng` offset test, drop the unused import.
5. Before changing the default 60, measure retry rate and time on a real Latin scan corpus.

### Assessment

**Ready to merge?** With fixes

**Reasoning:** The routing, config isolation and Tess4J changes are correct and well tested, and nothing breaks callers. But as shipped, every Datashare install without models would log a WARN per document, and every text-less image would cost about 8x today's OCR. Both are 15-minute fixes.
