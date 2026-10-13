# OCR confidence on every OCR'd document

Date: 2026-10-05
Status: approved design, pending implementation plan

## Problem

Datashare users want to find badly OCR'd documents (filter or sort on a score, then re-OCR or review them). Extract writes `ocr:confidence` (0 to 1) only on some OCR paths:

- `TESSERACT` with an explicit `ocrLanguage`, or with script models missing: no score. `AutoLanguageOCRParser` is skipped (`Extractor.java:288`, `Extractor.java:411`), so Tika runs in plain-text mode and reports nothing.
- PDFs with `ocrStrategy` other than `NO_OCR`: no score with either engine. Tika passes the OCR parser the per-page `RenderResult.getMetadata()` (`AbstractPDF2XHTML.doOCROnCurrentPage`, Tika 3.3.0), so the page score never reaches the PDF's metadata.
- Datashare indexes the value as text: its mapping has no numeric detection, only a date template (`datashare-index/src/main/resources/datashare_index_mappings.json`). A range query compares strings and breaks on values like `1.0E-4`.

## Goal

Every document extract OCRs carries `ocr:confidence`, and Datashare can range-query it.

## Success criteria

- `metadata.tika_metadata_ocr_confidence:<0.6` in Datashare returns low-confidence images and PDFs.
- An image OCR'd with an explicit `ocrLanguage` gets a score with `TESSERACT` and `TESS4J`, and its extracted text is byte-identical to today's.
- A PDF OCR'd page by page (`OCR_ONLY`, `OCR_AND_TEXT_EXTRACTION`, `AUTO`) gets `ocr:confidence` (mean of its pages) and `ocr:confidence_min` (worst page).
- A PDF inside an archive keeps its own score, not mixed with its siblings or parent.

## Decisions

| # | Question | Decision |
|---|---|---|
| 1 | Scale and field | Keep `ocr:confidence`, 0 to 1, indexed as `metadata.tika_metadata_ocr_confidence`. |
| 2 | Not actually OCR'd (`skipOcr`, PSM 0, outside min/max file size) | No score. A missing score means "not OCR'd". |
| 3 | Pages or images with no recognized text | Excluded from the PDF mean and min, so blank pages do not drag a scan to 0. A PDF where no page has text gets no score. |
| 4 | PDF aggregation | Unweighted mean of page scores, plus `ocr:confidence_min`. Page means are what both engines report. |
| 5 | Explicit `ocrLanguage` | Goes through `AutoLanguageOCRParser` in a fixed-language mode: no script detection, no retry, one `read()` with the configured language. |
| 6 | Datashare mapping | Map `metadata.tika_metadata_ocr_confidence` and `metadata.tika_metadata_ocr_confidence_min` as `float`. New indices only, existing projects need a reindex. |

## Design

### 1. Fixed-language mode in `AutoLanguageOCRParser`

`Extractor.withAutoLanguage` always wraps the OCR parser when OCR is on. It passes a fixed model when `ocrLanguage` is set, or when script models are missing (the configured language, today `eng`). In fixed mode, `parse` skips `detect` and the retry, calls `read(image, fixedModel, ...)`, and sets `OCR_MODEL` and `OCR_CONFIDENCE` as it does today. It sets no `OCR_SCRIPT`. `readsAsIs` keeps short-circuiting (decision 2).

`read()` already covers both engines: hOCR plus `HocrTextHandler` for `TESSERACT`, `OCR_CONFIDENCE` from metadata for `TESS4J`. With `TESS4J` and `skipConfidence=true`, no score is written, by the user's choice.

### 2. PDF page aggregation

- `OCRConfidence`: a small mutable accumulator (page score sum, page count, min). `add(double)`, `mean()`, `min()`, `isEmpty()`.
- `AutoLanguageOCRParser.parse`: after setting `OCR_CONFIDENCE` on a read with text, if the `ParseContext` holds an `OCRConfidence`, add the score to it.
- `OCRConfidencePDFParser`: a `ParserDecorator` around Tika's `PDFParser`, installed by `Extractor` through `replaceParser`. When the PDF config's `ocrStrategy` is not `NO_OCR`, it puts a fresh `OCRConfidence` in the context, delegates, writes `ocr:confidence` and `ocr:confidence_min` if the accumulator is not empty, then restores the context's previous value in a `finally`. Restoring keeps nested PDFs separate.
- `EmbedSpawner.copyParsingConfig` copies only named keys, so forked and deferred-OCR contexts never see the parent's accumulator. A test pins this.

With `NO_OCR` (the default), inline images are extracted as child documents (`Extractor.java:640`) and each already gets its own score. The decorator does nothing.

### 3. Datashare mapping (separate PR in `datashare`)

Add `float` properties for both fields under `metadata` in `datashare_index_mappings.json`, and bump the `extract.version` once extract is released.

## Testing

Unit tests in `extract-lib`, TDD, each seen failing first:

- `AutoLanguageOCRParserTest`: fixed mode sets `OCR_CONFIDENCE` and `OCR_MODEL`, runs no OSD pass, and produces the same text as the plain parser, for `TESSERACT` and `TESS4J`.
- `ExtractorTest`: an image with `ocrLanguage=eng` gets `ocr:confidence`. Missing script models give the same result.
- `OCRConfidencePDFParserTest`: a two-page scanned PDF with `OCR_ONLY` gets both fields. A PDF with one blank page is scored on its text page only. `NO_OCR` writes neither field.
- An archive holding two scanned PDFs: each gets its own score.
- `datashare`: an index mapping test asserting both fields are `float`.

## Known limits

- `ocrCache` hits carry no score: `CacheParserDecorator` stores text only. Images and PDFs served from the cache stay unscored until the cache is cleared or taught to store the score.
- Existing Datashare indices keep the text mapping until reindexed.

## Out of scope

- A confidence filter or column in the Datashare UI.
- Per-word or per-page scores in the index.
