# PDF OCR Strategy Option Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a configurable `ocrStrategy` extraction option so callers can opt into full-page rendering OCR for scanned/MRC PDFs, while the default behavior is unchanged.

**Architecture:** Add a public `setOcrStrategy(String)` setter on `Extractor` that maps a string to Tika's `PDFParserConfig.OCR_STRATEGY` and keeps `extractInlineImages` coherent with the chosen strategy (off for any rendering strategy, on for `NO_OCR`). Wire it into `configure()` as an option with default `NO_OCR`. Separately demote a noisy JBIG2 log to `DEBUG`.

**Tech Stack:** Java 17, Apache Tika 3.3.0 (`PDFParserConfig`), JUnit 4 + FEST assertions, Maven.

## Global Constraints

- Module: all changes are in `extract-lib`.
- Default OCR strategy is `NO_OCR` — existing behavior must be unchanged when the option is not set.
- Invalid `ocrStrategy` values must not throw; log a warning and fall back to `NO_OCR`.
- Do not commit any real/confidential sample document as a test fixture.
- Tesseract-dependent tests follow the existing pattern in `ExtractorTest` (they run as plain `@Test` methods; CI has Tesseract installed).
- Co-author trailer on commits: `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.

---

## File Structure

- `extract-lib/src/main/java/org/icij/extract/extractor/Extractor.java` — add `@Option` annotation, `setOcrStrategy(String)` setter, `getOcrStrategy()` and `isExtractInlineImages()` getters, and the `configure()` wiring.
- `extract-lib/src/main/java/org/icij/extract/ocr/ImageIOTranscodingOCRParser.java` — demote one log line from `WARN` to `DEBUG`.
- `extract-lib/src/test/java/org/icij/extract/extractor/ExtractorTest.java` — unit tests for the setter/coupling and option parsing, plus one Tesseract-dependent end-to-end test.

---

## Task 1: `setOcrStrategy` setter with inline-image coupling

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/extract/extractor/Extractor.java`
- Test: `extract-lib/src/test/java/org/icij/extract/extractor/ExtractorTest.java`

**Interfaces:**
- Produces:
  - `public void setOcrStrategy(final String strategy)` — sets `pdfConfig` OCR strategy from a case-insensitive name; unknown value falls back to `NO_OCR` and logs a warning.
  - `public PDFParserConfig.OCR_STRATEGY getOcrStrategy()` — returns the currently configured strategy.
  - `public boolean isExtractInlineImages()` — returns the current `pdfConfig.isExtractInlineImages()` value.
- Consumes: existing private field `pdfConfig` (`Extractor.java:145`), existing import `org.apache.tika.parser.pdf.PDFParserConfig` (`Extractor.java:26`), existing `logger` field used elsewhere in the class.

- [ ] **Step 1: Write the failing tests**

Add these tests to `ExtractorTest.java` (near the other OCR tests). They need no Tesseract — they assert configuration state only.

```java
@Test
public void testSetOcrStrategyDefaultsToNoOcrAndKeepsInlineImages() {
    Extractor extractor = new Extractor();
    assertThat(extractor.getOcrStrategy()).isEqualTo(PDFParserConfig.OCR_STRATEGY.NO_OCR);
    assertThat(extractor.isExtractInlineImages()).isTrue();
}

@Test
public void testSetOcrStrategyAutoDisablesInlineImages() {
    Extractor extractor = new Extractor();
    extractor.setOcrStrategy("AUTO");
    assertThat(extractor.getOcrStrategy()).isEqualTo(PDFParserConfig.OCR_STRATEGY.AUTO);
    assertThat(extractor.isExtractInlineImages()).isFalse();
}

@Test
public void testSetOcrStrategyIsCaseInsensitive() {
    Extractor extractor = new Extractor();
    extractor.setOcrStrategy("ocr_and_text_extraction");
    assertThat(extractor.getOcrStrategy())
            .isEqualTo(PDFParserConfig.OCR_STRATEGY.OCR_AND_TEXT_EXTRACTION);
    assertThat(extractor.isExtractInlineImages()).isFalse();
}

@Test
public void testSetOcrStrategyInvalidFallsBackToNoOcr() {
    Extractor extractor = new Extractor();
    extractor.setOcrStrategy("AUTO");
    extractor.setOcrStrategy("not-a-strategy");
    assertThat(extractor.getOcrStrategy()).isEqualTo(PDFParserConfig.OCR_STRATEGY.NO_OCR);
    assertThat(extractor.isExtractInlineImages()).isTrue();
}
```

Add the import if not already present at the top of `ExtractorTest.java`:

```java
import org.apache.tika.parser.pdf.PDFParserConfig;
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn -q -pl extract-lib test -Dtest=ExtractorTest#testSetOcrStrategyAutoDisablesInlineImages+testSetOcrStrategyDefaultsToNoOcrAndKeepsInlineImages+testSetOcrStrategyIsCaseInsensitive+testSetOcrStrategyInvalidFallsBackToNoOcr`
Expected: FAIL with compilation error — `cannot find symbol: method setOcrStrategy / getOcrStrategy / isExtractInlineImages`.

- [ ] **Step 3: Add the setter and getters**

In `Extractor.java`, add these methods near the other OCR setters (after `setOcrTimeout(Duration)`, around line 356):

```java
/**
 * Set the OCR strategy used for PDFs, mapping to Tika's {@link PDFParserConfig.OCR_STRATEGY}.
 *
 * <p>{@code NO_OCR} (the default) keeps the per-inline-image OCR path
 * ({@code extractInlineImages = true}). Any rendering strategy
 * ({@code AUTO}, {@code OCR_AND_TEXT_EXTRACTION}, {@code OCR_ONLY}) renders whole pages and
 * therefore disables inline-image extraction, so a scanned page is OCR'd once as a composite
 * rather than once per embedded layer. An unknown value falls back to {@code NO_OCR}.
 *
 * @param strategy the strategy name, case-insensitive
 */
public void setOcrStrategy(final String strategy) {
    PDFParserConfig.OCR_STRATEGY parsed;
    try {
        parsed = PDFParserConfig.OCR_STRATEGY.valueOf(strategy.trim().toUpperCase());
    } catch (final IllegalArgumentException e) {
        logger.warn("unknown ocrStrategy \"{}\"; falling back to NO_OCR", strategy);
        parsed = PDFParserConfig.OCR_STRATEGY.NO_OCR;
    }
    pdfConfig.setOcrStrategy(parsed);
    pdfConfig.setExtractInlineImages(parsed == PDFParserConfig.OCR_STRATEGY.NO_OCR);
}

/**
 * @return the OCR strategy currently configured for PDFs
 */
public PDFParserConfig.OCR_STRATEGY getOcrStrategy() {
    return pdfConfig.getOcrStrategy();
}

/**
 * @return whether inline images are extracted from PDFs for the current strategy
 */
public boolean isExtractInlineImages() {
    return pdfConfig.isExtractInlineImages();
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn -q -pl extract-lib test -Dtest=ExtractorTest#testSetOcrStrategyAutoDisablesInlineImages+testSetOcrStrategyDefaultsToNoOcrAndKeepsInlineImages+testSetOcrStrategyIsCaseInsensitive+testSetOcrStrategyInvalidFallsBackToNoOcr`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add extract-lib/src/main/java/org/icij/extract/extractor/Extractor.java extract-lib/src/test/java/org/icij/extract/extractor/ExtractorTest.java
git commit -m "$(printf 'feat(ocr): add Extractor.setOcrStrategy with inline-image coupling\n\nCo-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>')"
```

---

## Task 2: Wire `ocrStrategy` into `configure()` and `@Option`

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/extract/extractor/Extractor.java`
- Test: `extract-lib/src/test/java/org/icij/extract/extractor/ExtractorTest.java`

**Interfaces:**
- Consumes: `setOcrStrategy(String)` / `getOcrStrategy()` / `isExtractInlineImages()` from Task 1; the existing `Options<String>` API (`options.get(key, default).value()` returns `Optional<String>`, as used for `ocrLanguage` at `Extractor.java:210`); `Options.from(Map.of(...))` constructor pattern (used at `ExtractorTest.java:206`).
- Produces: `Options.from(Map.of("ocrStrategy", "AUTO"))` passed to `new Extractor(factory, options)` configures the strategy.

- [ ] **Step 1: Write the failing tests**

Add to `ExtractorTest.java`:

```java
@Test
public void testConfigureOcrStrategyFromOptions() {
    Extractor extractor = new Extractor(
            new DocumentFactory().withIdentifier(new PathIdentifier()),
            Options.from(Map.of("ocrStrategy", "AUTO")));
    assertThat(extractor.getOcrStrategy()).isEqualTo(PDFParserConfig.OCR_STRATEGY.AUTO);
    assertThat(extractor.isExtractInlineImages()).isFalse();
}

@Test
public void testConfigureOcrStrategyDefaultIsNoOcr() {
    Extractor extractor = new Extractor(
            new DocumentFactory().withIdentifier(new PathIdentifier()),
            Options.from(Map.of()));
    assertThat(extractor.getOcrStrategy()).isEqualTo(PDFParserConfig.OCR_STRATEGY.NO_OCR);
    assertThat(extractor.isExtractInlineImages()).isTrue();
}
```

If not already imported in `ExtractorTest.java`, add:

```java
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.PathIdentifier;
import org.icij.task.Options;
import java.util.Map;
```

(Check the existing imports first — `Options`, `Map`, `DocumentFactory`, and `PathIdentifier` are likely already imported because they are used by existing tests around `ExtractorTest.java:204-223`. Only add what is missing.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn -q -pl extract-lib test -Dtest=ExtractorTest#testConfigureOcrStrategyFromOptions+testConfigureOcrStrategyDefaultIsNoOcr`
Expected: FAIL — `testConfigureOcrStrategyFromOptions` fails because the option is ignored (strategy stays `NO_OCR`, `isExtractInlineImages()` stays `true`).

- [ ] **Step 3: Add the option wiring**

In `Extractor.java`, inside `configure()`, add this line immediately after the `ocrLanguage` line (`Extractor.java:210`) and before the `ocrTimeout` line:

```java
        options.get("ocrStrategy", "NO_OCR").value().ifPresent(this::setOcrStrategy);
```

Then add the `@Option` annotation alongside the other class-level annotations (near the `ocrLanguage` annotation, around `Extractor.java:99`):

```java
@Option(name = "ocrStrategy", description = "Set the PDF OCR strategy. One of \"NO_OCR\" " +
        "(default), \"AUTO\", \"OCR_AND_TEXT_EXTRACTION\" or \"OCR_ONLY\". Any rendering " +
        "strategy OCRs whole pages and disables inline-image extraction, which is required to " +
        "correctly extract scanned/MRC PDFs. Defaults to NO_OCR.", parameter = "strategy")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn -q -pl extract-lib test -Dtest=ExtractorTest#testConfigureOcrStrategyFromOptions+testConfigureOcrStrategyDefaultIsNoOcr`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add extract-lib/src/main/java/org/icij/extract/extractor/Extractor.java extract-lib/src/test/java/org/icij/extract/extractor/ExtractorTest.java
git commit -m "$(printf 'feat(ocr): accept ocrStrategy extraction option\n\nCo-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>')"
```

---

## Task 3: End-to-end OCR via the rendering strategy

**Files:**
- Test: `extract-lib/src/test/java/org/icij/extract/extractor/ExtractorTest.java`
- Uses existing fixture (read-only): `extract-lib/src/test/resources/documents/ocr/jbig2_scan.pdf` (a 2-page JBIG2 scan with no text layer; page 1 contains "Lorem ipsum dolor sit amet ...", page 2 contains "... 1234567890"). This fixture renders cleanly under PDFBox at the default OCR DPI, so the page-rendering path recovers its text reliably. (Do not use `embedded.pdf`: its embedded image is only 72 dpi native and blurs when upscaled to a 300-dpi page render, so OCR drops the first word — empirically verified.)

**Interfaces:**
- Consumes: `setOcrStrategy(String)` from Task 1; the existing static helper `extractDocument(extractor, path)` (`ExtractorTest.java:768`) and `aBasicExtractor()` (`ExtractorTest.java:764`); `Spewer.toString(Reader)` already imported in the test.

- [ ] **Step 1: Write the failing test**

This proves the page-rendering OCR path is wired through `extract`'s OCR parser and yields correct parent text. It depends on Tesseract, like the other OCR tests in this file (e.g. the existing `testOcrShouldExtractTextFromJbig2ScannedPdf`, which asserts the same two substrings on the same fixture via the default transcode path). Add to `ExtractorTest.java`, next to the other `jbig2_scan.pdf` tests:

```java
@Test
public void testOcrStrategyAutoRendersPagesAndExtractsText() throws Throwable {
    //GIVEN
    String text;
    Extractor extractor = aBasicExtractor();
    extractor.setOcrStrategy("AUTO");
    //WHEN
    TikaDocument tikaDocument = extractDocument(extractor, "/documents/ocr/jbig2_scan.pdf");
    try (Reader reader = tikaDocument.getReader()) {
        text = Spewer.toString(reader);
    }
    //THEN: with a rendering strategy, inline-image extraction is off and Tika renders whole
    // pages for OCR, recovering the scanned text across both pages.
    assertThat(text).contains("Lorem ipsum");
    assertThat(text).contains("1234567890");
}
```

Note: do NOT assert on `OCR_PARSER` metadata here. Under a rendering strategy, Tika OCRs the rendered page internally (`doOCROnCurrentPage`) rather than through `extract`'s `EmbedSpawner`, so the `OCR_PARSER` metadata tag is not propagated to the parent document. That propagation is a characteristic of the per-image path only and is out of scope for this change; the meaningful contract here is recovered text. Assert `contains(...)` substrings (matching the established `jbig2_scan.pdf` test) rather than exact text — OCR/rendering whitespace varies.

- [ ] **Step 2: Run the test to verify it passes, and confirm it exercises the rendering path**

Run: `mvn -q -pl extract-lib test -Dtest=ExtractorTest#testOcrStrategyAutoRendersPagesAndExtractsText`
Expected: PASS once Tasks 1-2 are in place, because the production code already supports `AUTO`. To confirm the test genuinely exercises an OCR path (and is not a no-op), temporarily change the call to `extractor.setOcrStrategy("NO_OCR")`, run it, and verify it still passes (this fixture's JBIG2 is self-contained, so the per-image transcode path also recovers the text); then restore `"AUTO"`. (Do not commit the temporary change.)

- [ ] **Step 3: No new production code**

Tasks 1-2 already implement the behavior. This task only adds the end-to-end guard. If the test fails under `AUTO`, do not weaken the text assertions to make it pass — investigate (confirm the rendered page reaches Tesseract) and report.

- [ ] **Step 4: Run the test to verify it passes**

Run: `mvn -q -pl extract-lib test -Dtest=ExtractorTest#testOcrStrategyAutoRendersPagesAndExtractsText`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add extract-lib/src/test/java/org/icij/extract/extractor/ExtractorTest.java
git commit -m "$(printf 'test(ocr): end-to-end AUTO strategy recovers page text\n\nCo-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>')"
```

---

## Task 4: Demote JBIG2 transcoding warning to DEBUG

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/extract/ocr/ImageIOTranscodingOCRParser.java:98-100`

**Interfaces:**
- Consumes: existing `LOGGER` field in `ImageIOTranscodingOCRParser`.
- Produces: no API change; behavior change is log-level only.

- [ ] **Step 1: Change the log level**

In `ImageIOTranscodingOCRParser.java`, change the log call at line 98 from `warn` to `debug`. Replace:

```java
            LOGGER.warn("could not decode image of type {} (resource: {}) for OCR; emitting no content",
                    metadata.get(Metadata.CONTENT_TYPE), metadata.get(RESOURCE_NAME_KEY));
```

with:

```java
            LOGGER.debug("could not decode image of type {} (resource: {}) for OCR; emitting no content",
                    metadata.get(Metadata.CONTENT_TYPE), metadata.get(RESOURCE_NAME_KEY));
```

Rationale to keep in mind (no code comment needed): for PDF JBIG2 images this is an expected, non-actionable outcome — the embedded stream lacks its separate `JBIG2Globals` symbol dictionary — and it is emitted once per stencil, so at `WARN` it floods logs on scanned documents.

- [ ] **Step 2: Verify the module still compiles and tests pass**

Run: `mvn -q -pl extract-lib test -Dtest=ImageIOTranscodingOCRParserTest`
Expected: PASS (existing transcoding tests are unaffected by the log level). If a test asserts on the warning being logged, update that assertion to expect the new level; otherwise leave the tests unchanged.

- [ ] **Step 3: Commit**

```bash
git add extract-lib/src/main/java/org/icij/extract/ocr/ImageIOTranscodingOCRParser.java
git commit -m "$(printf 'fix(ocr): demote undecodable-JBIG2 log from WARN to DEBUG\n\nExpected, non-actionable for globals-based embedded JBIG2; was flooding\nlogs once per stencil on scanned PDFs.\n\nCo-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>')"
```

---

## Task 5: Full module verification

**Files:** none (verification only).

- [ ] **Step 1: Run the full extract-lib test suite**

Run: `mvn -q -pl extract-lib test`
Expected: BUILD SUCCESS, no regressions. (Redis is required by parts of the suite per the README; ensure it is running, as for any test run in this repo.)

- [ ] **Step 2: Confirm no default behavior changed**

Confirm the existing OCR tests that rely on the default strategy (`testOcr`, the `embedded.pdf` "HEAVY METAL" test, `testIgnoreEmbeds`, `testDisableOcrOnEmbed`) still pass unchanged. These exercise the `NO_OCR` default and prove backward compatibility.

- [ ] **Step 3: Final commit if any verification fix was needed**

If steps 1-2 required a fix, commit it:

```bash
git add -A
git commit -m "$(printf 'test(ocr): verification fixups for ocrStrategy option\n\nCo-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>')"
```

If no fix was needed, skip this step.

---

## Self-Review

**Spec coverage:**
- Option (name, default, enum mapping, new setter, invalid→fallback) → Tasks 1 & 2.
- Strategy/inline-image coupling → Task 1.
- Backward-compatible default `NO_OCR` → Tasks 1, 2, 5.
- End-to-end rendering OCR through extract's parser → Task 3.
- JBIG2 warning → DEBUG → Task 4.
- Compatibility/retrieval analysis → no code change required (inline images are concatenated, not spawned; attachments unaffected); guarded by the unchanged default behavior verified in Task 5.
- Out of scope (Datashare plumbing, DPI tuning, removing the transcoder, changing the default) → intentionally not in any task.

**Placeholder scan:** No TBD/TODO; every code step shows complete code; every command has an expected result.

**Type consistency:** `setOcrStrategy(String)`, `getOcrStrategy()` returning `PDFParserConfig.OCR_STRATEGY`, and `isExtractInlineImages()` returning `boolean` are used identically across Tasks 1-3. Option key `"ocrStrategy"` matches between the `@Option` annotation and the `configure()` lookup.
