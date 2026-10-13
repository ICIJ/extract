# PDF OCR strategy option (`ocrStrategy`)

Date: 2026-06-24
Status: Proposed
Module: `extract-lib`

## Problem

Scanned PDFs produced with Mixed Raster Content (MRC) compression extract as gibberish, and emit repeated `ImageIOTranscodingOCRParser` warnings of the form "could not decode image of type image/x-jbig2 ... emitting no content".

An MRC-compressed scanned page is not a single image. It is a stack of layers: a full-page continuous-tone background (typically JPEG) with the text removed or blurred out, plus one or more sharp 1-bit JBIG2 stencil masks that hold the actual text-glyph shapes. A page commonly contains dozens of small JBIG2 stencil strips, one per line or word region. Only the composite of all layers is human-readable.

`extract` currently configures the PDF parser (`Extractor.java`) to OCR embedded images individually rather than rendering pages:

```java
pdfConfig.setExtractInlineImages(true);
pdfConfig.setOcrStrategy(PDFParserConfig.OCR_STRATEGY.NO_OCR);
```

On an MRC scan this is exactly the wrong strategy, and it fails in two ways at once:

- The full-page JPEG background layer is OCR'd in isolation. Because the sharp text is not in this layer, Tesseract reads the residual smudges and noise and emits gibberish. This gibberish is what lands in the extracted text.
- Each JBIG2 stencil is handed to `ImageIOTranscodingOCRParser`, which cannot decode it (see next section), so it emits nothing and logs a warning per stencil.

The net result is a document whose real text is never recovered, padded with gibberish, accompanied by many warnings.

## Why not just fix JBIG2 transcoding

`ImageIOTranscodingOCRParser` exists to handle image formats that an ImageIO reader can decode but Tesseract cannot consume directly (WBMP, PCX, the non-PPM PNM variants, and self-contained JBIG2). It decodes the embedded stream to a `BufferedImage`, re-encodes it as PNG, and OCRs that. This works for self-contained formats.

It cannot work for the JBIG2 images in MRC PDFs, because those images are not self-contained. In a PDF, JBIG2 images share a symbol dictionary stored in a separate `JBIG2Globals` stream referenced from each image's `DecodeParms`. The per-image stream contains only symbol references; the symbol definitions live in the globals object. When Tika extracts an embedded JBIG2 image, it emits only that image's stream bytes (for example `image34.jb2`) without the globals. The decoder therefore receives an incomplete stream and cannot reconstruct the glyphs. No ImageIO reader (levigo or pdfbox-jbig2) can decode it, because the symbol data is simply absent from the bytes it is given.

Reuniting an image stream with its globals is only possible in the whole-PDF context, which is precisely where PDFBox's page renderer operates. Fixing transcoding would mean reimplementing page rendering. Two further reasons make rendering the correct path even if decoding were possible: the stencils are many small fragments that, OCR'd independently, produce fragmented and out-of-order text, whereas full-page rendering composites them in correct spatial position; and the blurry JPEG background would still OCR to gibberish under the per-image path.

`ImageIOTranscodingOCRParser` is retained unchanged as a fallback for self-contained JBIG2 and the other transcodable formats. It is simply not the fix for globals-based JBIG2 scans.

## Goal

Make the PDF OCR strategy configurable at extraction time, so callers (notably Datashare, per project, at index time) can opt into full-page rendering OCR for scanned/image-based PDFs, while the default behavior is unchanged.

## Design

### The option

Add an `ocrStrategy` option to `Extractor`, mirroring the existing `ocrType` / `ocrLanguage` / `embedHandling` pattern.

- An `@Option(name = "ocrStrategy", ...)` annotation documents it for the CLI, alongside the other option annotations on the class.
- In `configure()`:

```java
options.get("ocrStrategy", "NO_OCR").value()
    .ifPresent(this::setOcrStrategy);
```

- A new public setter accepts the value, so the library is usable programmatically and not only via string options:

```java
public void setOcrStrategy(final String strategy) { ... }
```

- Accepted values map to `PDFParserConfig.OCR_STRATEGY`: `NO_OCR` (default), `AUTO`, `OCR_AND_TEXT_EXTRACTION`, `OCR_ONLY`. Matching is case-insensitive.
- An unknown or unparseable value logs a warning and falls back to `NO_OCR` rather than throwing, so a malformed configuration cannot abort indexing.

### Strategy / inline-image coupling

The setter keeps the two related PDF settings coherent. This is the core behavioral logic:

- `NO_OCR` (default): `extractInlineImages(true)`. Identical to today's per-image OCR path. Nothing changes.
- Any rendering strategy (`AUTO`, `OCR_AND_TEXT_EXTRACTION`, `OCR_ONLY`): `extractInlineImages(false)`.

Flipping inline-image extraction off under a rendering strategy is required to prevent double OCR. Without it, Tika would render and OCR the page and also OCR the embedded background image, so the gibberish would be concatenated alongside the correct text. With it, each scanned page is OCR'd exactly once, as a composited whole.

### Compatibility and retrieval analysis

The default is unchanged, so existing deployments behave exactly as before unless they explicitly pass a rendering strategy.

Flipping `extractInlineImages` off under a rendering strategy does not remove any retrievable content from Datashare. In `extract`'s SPAWN mode, `EmbedSpawner.parseEmbedded` treats inline images (Tika `EmbeddedResourceType.INLINE`, which is what PDF inline images are) by concatenating their parsed text into the parent document; it does not spawn them as separate child documents. Inline images are therefore not independently retrievable today regardless of this option; they only ever contributed concatenated text. Under a rendering strategy that concatenated text is replaced by superior full-page OCR text on the same parent document.

True PDF file attachments are non-inline embedded resources. They are spawned as child documents through `spawnEmbedded` and are not governed by `extractInlineImages`. Their extraction and retrieval are unaffected by this option.

Consequently a mixed index (some documents extracted under `NO_OCR`, others under a rendering strategy) remains coherent: the only observable difference is that rendering-strategy documents carry clean parent text instead of gibberish.

Edge case to record: a born-digital PDF that embeds a genuine photo as an inline image has that photo's text concatenated into the parent today. Under `AUTO`, such a page has a real text layer and is not rendered, so the photo is not OCR'd. This is niche, and `AUTO` is the appropriate trade-off for pages that already have extractable text.

### JBIG2 warning

Independently of the strategy work, demote the `ImageIOTranscodingOCRParser` "could not decode image ... emitting no content" log from `WARN` to `DEBUG`. For globals-based embedded JBIG2 this outcome is expected and not actionable, and it is emitted once per stencil (many times per scanned document). Under a rendering strategy the transcoder is not invoked for these images at all, so the warning disappears on its own; for deployments staying on `NO_OCR` the demotion removes log noise. This is a small, self-contained change that can land independently of the option.

## Testing

Unit tests, which directly guard the code being added:

- Option parsing: `ocrStrategy=AUTO` (and other valid values, case-insensitively) resolves to the corresponding `OCR_STRATEGY`; an invalid value falls back to `NO_OCR` and logs a warning rather than throwing.
- Strategy / inline-image coupling: after `setOcrStrategy("NO_OCR")` the PDF config has `extractInlineImages == true`; after any rendering strategy it has `extractInlineImages == false`.

End-to-end test, following the existing Tesseract-dependent OCR test pattern in this module:

- Use a small, synthetic, non-confidential image-only PDF (a one-page PDF whose only content is a rasterized image of plain text, with no text layer). It may be generated in-test via PDFBox or committed as a fixture under `extract-lib/src/test/resources/documents/ocr/`.
- Assert that under `ocrStrategy=AUTO` the parent document's extracted text contains the known rendered string, confirming the page-render OCR path is wired through `extract`'s OCR parser.

The MRC-specific behavior (layered JPEG background plus globals-based JBIG2 stencils) is validated manually against a representative scanned sample. No such sample is committed, as representative documents are confidential.

## Out of scope

- Datashare-side plumbing to pass `ocrStrategy` at index time lives in the Datashare repository and is a separate change. This spec only makes `extract` accept and honor the option.
- OCR render DPI tuning. Tika's default render DPI is adequate for the validated case; exposing a DPI option is deferred (YAGNI).
- Removing or rewriting `ImageIOTranscodingOCRParser`. It remains as a fallback for self-contained transcodable images.
- Changing the default strategy. The default stays `NO_OCR` for backward compatibility.
