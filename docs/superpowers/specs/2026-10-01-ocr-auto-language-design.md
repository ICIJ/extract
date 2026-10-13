# OCR automatic language selection

Date: 2026-10-01
Status: approved design, pending implementation plan

## Problem

When no `ocrLanguage` is given, extract OCRs every image with `eng` (`Extractor.java:254`, `Extractor.java:279`). Images in other scripts come out as Latin-letter garbage, which Datashare's language detector then labels with random languages. Datashare indexes millions of documents from unknown datasets, so a per-dataset language setting does not work.

## Goal

When `ocrLanguage` is absent, extract picks the tesseract model per image so the text comes out in the right script, with no user setting, at millions-of-documents scale. Each image records what was detected and how confident the OCR was, so Datashare can improve its language detection later.

## Success criteria

- Every synthetic fixture (Latin, Cyrillic, Arabic, Simplified Chinese, Japanese, Korean, mixed) is read in the right script with both `TESSERACT` and `TESS4J`.
- An explicit `ocrLanguage` behaves exactly as today.
- Missing `osd` or script models: today's `eng` behavior, one warning at startup, no per-image failure.
- Latin images cost about one detection pass (~0.25 s) more than today.

## Evidence (local measurements, tesseract 5)

Measured by hand on a local Simplified Chinese image corpus (126 images with text, not committed), an English control and a synthetic Chinese + Russian + English image:

- OSD (`--psm 0 -l osd`) named the right script for 124/126 images. Its confidence is unreliable: wrong answers scored in the same range as right ones.
- Script models (`Latin`, `HanS`, `Cyrillic`, `Arabic`, `Japanese`, `Hangul`, ...) read every language of their script.
- Mean word confidence separates right from wrong reads: lowest correct read 67.6, wrong-script reads 16 to 57.
- Confidence alone picked `HanT` for 12 Simplified images and produced Traditional characters absent from the image.
- OSD reports one script for mixed images. Combined models (`HanS+Cyrillic`) read all scripts, with rare cross-script slips.
- Cost per image: OSD 0.23 s, `eng` 0.22 s, `chi_sim+eng` 0.48 s, combined set 2 to 4x one model.

## Decisions

| # | Question | Decision |
|---|---|---|
| 1 | Routing strategy | Detect script, OCR with `<script>+Latin`, retry once with the combined set if confidence is low, keep the higher-scoring pass. |
| 2 | Han | `HanS` only. `HanT` is never used automatically. Traditional-Chinese users set `ocrLanguage=chi_tra`. |
| 3 | Retry threshold | New option `ocrRetryConfidence`, 0 to 100, default 60. |
| 4 | Confidence in `TESSERACT` | Run Tika in hOCR mode for auto-language images, read `x_wconf`, rebuild plain text. |
| 5 | Tess4J models | One cached `Tesseract` instance per model string. |
| 6 | Validation set | Synthetic PNGs written from scratch, rendered once by hand, committed. No generator script, no gated tests, no local metrics script. |

## Design

### Activation

- `configure` reads `options.valueIfPresent("ocrLanguage")`. Absent: auto mode. Present: today's behavior.
- `setOcrLanguage(...)` called programmatically turns auto mode off.
- The constructor default `eng` stays as the fallback language.
- `ocrRetryConfidence` is ignored when `ocrLanguage` is set.

### Components

- `ocr/AutoLanguageOCRParser` (new): wraps the configured OCR parser, the same way `ImageIOTranscodingOCRParser` does. Installed in `setOcrConfig` only when auto mode is on and the startup check passes.
- Wrapping order: the cache (`ocrCache`, `Extractor.java:298`) wraps the router, so cached results are routed results. `ImageIOTranscodingOCRParser` receives the router instead of the raw parser, so transcoded images (JBIG2, ...) are routed too.
- hOCR confidence handler (new, `TESSERACT` only): a SAX handler that reads `x_wconf` from `ocrx_word` elements, computes the mean, and rebuilds plain text: one line per `ocr_line`, a blank line between `ocr_par`. Tika's `HOCRPassThroughHandler` forwards element attributes, so `title="... x_wconf NN"` reaches it.
- `Tess4JOCRParser` changes:
  - `extractOSDOutput` also records `tess:script` and `tess:script_confidence` (today it records orientation only, `Tess4JOCRParser.java:469`).
  - One cached `Tesseract` per model string, replacing the single instance fixed in `getOrInit` (`Tess4JOCRParser.java:337`). Safe because all OCR runs on one thread (`Tess4JOCRParser.java:84`).
  - The language comes from the per-call config.
  - `skipConfidence` is forced off in auto mode.

### Per-image flow

1. **Detect.** Delegate parse with a cloned `TesseractOCRConfig` at PSM 0 into a scratch `Metadata`. Read `tess:script` and `tess:script_confidence`.
2. **Map script to model.**

   | OSD script | Model |
   |---|---|
   | `Latin` | `Latin` |
   | `Han` | `HanS` |
   | `Cyrillic` | `Cyrillic` |
   | `Arabic` | `Arabic` |
   | `Hangul` | `Hangul` |
   | `Japanese`, `Hiragana`, `Katakana` | `Japanese` |
   | detection failed or no text | `Latin` |
   | any other script | skip to pass 2 |

3. **Pass 1.** OCR with `<model>+Latin` (or `Latin` alone) into a buffer. Record mean confidence (0 to 100).
4. **Pass 2,** only if pass 1 confidence < `ocrRetryConfidence`. OCR with `Latin+HanS+Cyrillic+Arabic+Japanese+Hangul` into a buffer. Keep the pass with the higher confidence.
5. **Emit.** Replay the winning text to the real handler and write the metadata.

Buffering is required because text already sent to the handler cannot be withdrawn when a retry wins.

### Metadata (auto mode only)

| Key | Example | Meaning |
|---|---|---|
| `ocr:script` | `Han` | Raw OSD script name. Absent when detection failed. |
| `ocr:script_confidence` | `0.76` | Raw OSD confidence. Recorded only, never used for routing. |
| `ocr:model` | `HanS+Latin` | Model string of the winning pass. |
| `ocr:confidence` | `0.82` | Existing property (`ParserWithConfidence.java:7`), 0 to 1 scale as Tess4J already writes it (`Tess4JOCRParser.java:639`). |

Tika's own `tess:*` keys from the detection pass stay in the scratch `Metadata` and do not reach the document.

### Startup check

- Runs once per `Extractor`, when the OCR config is set, in auto mode only.
- Required models: `osd`, `Latin`, `HanS`, `Cyrillic`, `Arabic`, `Japanese`, `Hangul`.
- Installed list: `TesseractOCRParser.getLangs()` for `TESSERACT`, the `*.traineddata` files in the tessdata directory for `TESS4J`.
- A pure function `missingModels(installed)` returns the missing set.
- All-or-nothing: if anything is missing, log one `WARN` naming the missing models, install no router, and run plain `eng` exactly as today.

### Configuration

- New `@Option ocrRetryConfidence` next to `ocrLanguage` (`Extractor.java:107`): 0 to 100, default 60. Out-of-range values are clamped with a warning, like `maxEmbedDepth`.
- `ocrLanguage` description updated: when absent, the language is chosen per image.

### Error handling

- Detection failure: route to `Latin`, no `ocr:script`.
- OCR pass failure: propagates as today, so the `OCRParserAdapter` `finally` that restores the media type still runs.
- Missing models: startup fallback above, never a per-image error.

### Behavior changes

- Users not passing `ocrLanguage` who have the models installed get auto mode and about +0.25 s detection per image.
- Auto-mode `TESSERACT` text comes from hOCR, so whitespace may differ slightly from today's plain-text output.
- Without script models installed (Datashare's Docker image today), behavior is unchanged.

## Testing

### Fixtures

Seven PNGs in `extract-lib/src/test/resources/documents/ocr/auto/`: `latin`, `cyrillic`, `arabic`, `hans`, `japanese`, `hangul`, `mixed` (Chinese + Russian + English). Each has 2 or 3 short sentences written from scratch, rendered once with ImageMagick and Noto fonts. No generator is committed. Each test holds its expected sentence as a string.

### Tests (all ungated)

1. Routing, per fixture, parameterized over `TESSERACT` and `TESS4J`: expected `ocr:script`, expected `ocr:model`, known words present, `ocr:confidence >= 0.6`.
2. Mixed image: Chinese, Russian and English words present (no assertion on which pass won).
3. Retry: `ocrRetryConfidence=0` keeps the pass 1 model. A stub delegate with a low pass 1 score shows pass 2 runs and the higher-scoring pass wins.
4. hOCR handler: a hand-written hOCR string gives the expected plain text and mean `x_wconf`.
5. Startup check: `missingModels` returns the missing set. An `Extractor` with a missing model logs one `WARN` and writes no `ocr:model`.
6. Explicit `ocrLanguage=eng`: no routing metadata, same text as today.
7. Tess4J OSD fills `tess:script`.

Existing OCR tests (`ExtractorTest`, `ocr/*Test`) are left as is and run in auto mode as an English regression check. A whitespace-only failure gets its expected text updated and flagged in the PR. Any other failure is a bug.

### CI

Extend the apt line in `.circleci/config.yml:40` with `tesseract-ocr-osd tesseract-ocr-script-latn tesseract-ocr-script-hans tesseract-ocr-script-cyrl tesseract-ocr-script-arab tesseract-ocr-script-jpan tesseract-ocr-script-hang`.

## Out of scope (follow-ups)

- Datashare: bump the extract version.
- Datashare: package `tesseract-ocr-osd` and the script models in Docker, snap and Windows.
- Datashare: feed `ocr:script`, `ocr:model` and `ocr:confidence` into Lingua (one-language script, narrowed candidates, low confidence as UNKNOWN).
- Partial routing when only some script models are installed.
- `HanT` routing for Traditional Chinese datasets.
