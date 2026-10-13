# OCR page segmentation mode: blank-read fallback and `ocrPageSegMode` option

Date: 2026-10-11
Repos: extract (extract-lib), datashare

## Problem

Tesseract runs with Tika's default page segmentation mode, `1` (`TesseractOCRConfig.pageSegMode = "1"`, Tika 3.3.0). Mode 1 looks for a page layout. On images without one, such as dark-mode chat screenshots with text in scattered bubbles, it finds no text block and returns nothing.

Measured on a `.doc` holding 27 screenshots (Tesseract 5.3.4, `-l HanS`, no preprocessing):

| Mode | Blank images | CJK characters |
|---|---|---|
| 1 (today) | 5 | 2,794 |
| 6 | 0 | 6,084 |
| 11 | 0 | 5,682 |

The existing language retry in `AutoLanguageOCRParser` does not help: it skips blank reads (`!best.text().isEmpty()`) and only changes the language model.

Confidence cannot drive this choice. On several images mode 1 reads a few words with high confidence (76 to 79) and misses most of the text, so "retry below N" never fires and "keep the higher confidence" keeps the poor read. A blank read is the only safe trigger: it cannot get worse.

Separately, OCR metadata is not auditable today: `ocr:model` and `ocr:confidence` are missing from every image OCR'd by the fan-out. The fan-out merges parse-derived fields back into the shared metadata when the embed's text is read (`EmbedSpawner`, `mergeParseDerivedFields` in the reader supplier), and Datashare's `ElasticsearchSpewer.getDocument` builds the metadata map before it reads the content.

## Goals

- Images that are blank under mode 1 get text when another mode finds some.
- Images that read non-blank under mode 1 are processed exactly as today: one Tesseract run, same text.
- Users can force a mode from the Datashare CLI and properties.
- Every OCR'd document records the mode that produced its text, and that record reaches the index.

## Non-goals

- UI exposure of the option.
- Changing the first-read language model (for example `HanS` alone instead of `HanS+Latin`).
- Image preprocessing (upscale, binarize, invert).
- Improving images that read non-blank but poorly under mode 1. Users with such corpora force a mode.

## Design: extract

### Option `ocrPageSegMode`

Declared on `Extractor` next to `ocrRetryConfidence` and read in `Extractor.configure()`.

- Absent or `AUTO` (case-insensitive): modes `[1, 6, 11]`.
- An integer in `1`, `3` to `13`: modes `[n]`.
- Anything else, including `0` (orientation and script detection only, never returns text) and `2` (not implemented in Tesseract): `IllegalArgumentException` naming the allowed values.

Why these three modes: `1` finds a layout, `6` assumes one block of text, `11` finds scattered text. The other modes duplicate one of these (`3` and `4` behave like `1`, `12` like `11`) or assume the image is a single line, word or character (`5`, `7` to `10`, `13`), which fails on full images.

### `PageSegModeOCRParser`

New class in `org.icij.extract.ocr`. Wraps the raw OCR parser (`ocrConfig.buildParser()`), always installed, under `AutoLanguageOCRParser` when that one is active:

```
AutoLanguageOCRParser (when the language is chosen per image)
  └─ PageSegModeOCRParser
       └─ OCRParserAdapter → Tesseract / Tess4J
```

`Extractor.setOcrConfig` builds `withAutoLanguage(ocrConfig, new PageSegModeOCRParser(ocrConfig.buildParser(), modes))`. `replaceParser` gains `PageSegModeOCRParser.class` so a reconfiguration swaps it like the other OCR parsers.

Constructor: `PageSegModeOCRParser(Parser delegate, List<String> modes)`.

`parse(stream, handler, metadata, context)`:

1. Read the caller's `TesseractOCRConfig` from the context (a new default one if absent). If its mode is `0`, delegate as is and return: this is `AutoLanguageOCRParser`'s script-detection pass.
2. Spool the stream to a file (`TikaInputStream.get(stream, tmp, metadata).getPath()`) so it can be read several times.
3. For each mode in `modes`: clone the config (`SerializationUtils.clone`), set the mode, put the clone in the context, and parse into a SAX recorder. Stop at the first read whose recorded characters are not all whitespace.
4. Replay the winning read, or the last read when all are blank, into `handler`.
5. Set `ocr:page_seg_mode` (`Property.externalInteger`) on `metadata` to the mode of the replayed read.
6. In a `finally`, restore the caller's config in the context.

Each read writes to the same `metadata`, so the last read's fields win, which is the replayed one. The recorder stores `startElement`, `endElement`, `characters` and `ignorableWhitespace` events and replays them in order; it works for both the text and hOCR handlers since it does not interpret the events.

Exceptions from a read propagate unchanged, as today. Each read has its own `ocrTimeout`, so a blank image costs at most 3 timeouts in `AUTO`.

### `AutoLanguageOCRParser`

Its language retry runs on a metadata clone, so the winning read's mode must be carried explicitly:

- `Pass` gains `pageSegMode`, read from `metadata.get(ocr:page_seg_mode)` after each read.
- After choosing the best read, `parse` sets `ocr:page_seg_mode` alongside `ocr:model` and `ocr:confidence`.

No other behavior change. Its first read now returns mode-6 or mode-11 text where it used to get nothing, so its existing retry can fire on those images.

### Tests (extract)

Unit tests on `PageSegModeOCRParser` with a stub delegate that returns text depending on the context's mode, in the style of `AutoLanguageOCRParserTest.StubOcr`, counting calls:

- Non-blank at 1: one call, mode-1 text, `ocr:page_seg_mode=1`.
- Blank at 1, text at 6: two calls, mode-6 text, `=6`.
- Blank at 1 and 6, text at 11: three calls, `=11`.
- All blank: three calls, empty output, `=11`.
- Modes `[4]`: one call with mode 4, `=4`, even when blank.
- Context mode `0`: one call, passed through, no `ocr:page_seg_mode`.
- The caller's config is restored in the context after a read and after an exception.

`AutoLanguageOCRParserTest`: when the language retry wins, `ocr:page_seg_mode` is the retry's mode.

`Extractor` option parsing: `AUTO`, `auto`, `6`, absent; rejected `0`, `2`, `14`, `abc`.

`Extractor` wiring: `PageSegModeOCRParser` sits under `AutoLanguageOCRParser` in auto-language mode, and is the OCR parser with a fixed `ocrLanguage`.

## Design: datashare

### Audit fix in `ElasticsearchSpewer`

In `getDocument`, read the content before calling `getMetadata(document)`, so parse-derived fields merged when the fan-out reader is read reach the metadata map. Applies to the existing `ocr:model` and `ocr:confidence` as well as `ocr:page_seg_mode`.

Test: an embed whose metadata gains `ocr:model` only when its reader is read is indexed with `tika_metadata_ocr_model`.

### Option and property `ocrPageSegMode`

String, no default in Datashare (a default would override the `-s` settings file); extract applies `AUTO`.

- `DatashareCliOptions`: `OCR_PAGE_SEG_MODE_OPT` and `ocrPageSegMode(parser)`, registered in `DatashareCli` next to `ocrRetryConfidence`.
- `PipelineOptions`: `--ocrPageSegMode` field and `putIfNotNull` in the properties mapping.
- `IndexTask`: no change; it forwards the options `Extractor` declares.

Help text: "Tesseract page segmentation mode. AUTO reads with mode 1, then 6 when the read is blank, then 11. An integer from 1 to 13 (except 2) forces that mode. Defaults to AUTO."

Validation happens in extract only, as for `ocrRetryConfidence`: a bad value fails the index task with extract's message.

### Tests (datashare)

- `DatashareCliTest`, `StageRunCommandTest`: the flag reaches the properties; absent when not given.
- `IndexTaskTest`: options include `ocrPageSegMode=`.
- `ElasticsearchSpewerTest`: the audit fix above.

## Delivery

1. extract PR: option, `PageSegModeOCRParser`, `AutoLanguageOCRParser` change. Release 9.18.0.
2. datashare PR A: `ElasticsearchSpewer` read-order fix. Independent of extract.
3. datashare PR B: bump extract to 9.18.0, CLI and property wiring.

## Behavior changes

- Images non-blank at mode 1: unchanged text, one run; metadata gains `ocr:page_seg_mode=1`.
- Images blank at mode 1: up to 2 extra Tesseract runs; text from mode 6 or 11 when one finds any.
- Indexed fan-out images gain `ocr:model`, `ocr:confidence`, `ocr:page_seg_mode` in their metadata.
- `--ocrPageSegMode n`: every image read once with mode `n`, no fallback.
