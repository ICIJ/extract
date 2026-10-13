# OCR Automatic Language Selection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** When `ocrLanguage` is absent, extract detects each image's script with tesseract OSD, OCRs it with the matching script model plus Latin, retries once with every script when confidence is low, and records `ocr:script`, `ocr:script_confidence`, `ocr:model` and `ocr:confidence`.

**Architecture:** A new `AutoLanguageOCRParser` wraps the configured OCR parser (`OCRParserAdapter`) inside the Extractor's parser tree. Per image it runs a PSM 0 detection pass into a scratch `Metadata`, then one or two OCR passes into buffers, and replays the winning text. `TESSERACT` passes run in hOCR mode and a new `HocrTextHandler` rebuilds plain text and the mean `x_wconf`; `TESS4J` passes read the `ocr:confidence` the parser already writes. The Extractor installs the router only when `ocrLanguage` is absent and every required model is installed.

**Tech Stack:** Java 17, Apache Tika 3.3.0 (`TesseractOCRParser`), Tess4J 5.15.0, tesseract 5, JUnit 4, fest-assert, logback `ListAppender`, commons-lang3 (already used in `Pair.java`).

**Spec:** `docs/superpowers/specs/2026-10-01-ocr-auto-language-design.md`

## Global Constraints

- Model names always use the `script/` prefix: `script/Latin`, `script/HanS`, `script/Cyrillic`, `script/Arabic`, `script/Japanese`, `script/Hangul` (decided 2026-10-01: Tika rejects bare `Latin`/`HanS`, see Spec deviations).
- Required models: `osd`, `script/Latin`, `script/HanS`, `script/Cyrillic`, `script/Arabic`, `script/Japanese`, `script/Hangul`. All-or-nothing.
- Combined set: `script/Latin+script/HanS+script/Cyrillic+script/Arabic+script/Japanese+script/Hangul`.
- `HanT` is never used automatically.
- `ocrRetryConfidence`: 0 to 100, default 60, out-of-range clamped with a warning.
- Metadata keys, auto mode only: `ocr:script` (raw OSD name), `ocr:script_confidence` (raw OSD value), `ocr:model` (winning model string), `ocr:confidence` (0 to 1).
- `tess:*` keys from the detection pass never reach the document.
- An explicit `ocrLanguage` behaves exactly as today.
- No new dependencies.
- Tests: JUnit 4, `org.fest.assertions.Assertions.assertThat`, `// Given / // When / // Then` blocks, method names `test_<behavior>`. All ungated.
- Code style: no comments except a non-obvious why. Never leave `ponytail:` comments.
- Commit with the `/commit` skill, on branch `feat/ocr-auto-language`.
- Run Maven in the background and poll; never block on it.

## Spec deviations (found while planning, verified against code)

1. **`script/` model names.** Tika validates `-l` against `([a-zA-Z]{3}(_[a-zA-Z]{3,4}){0,2})|script(/|\\)[A-Z][a-zA-Z_]+` (`TesseractOCRConfig.java:124`, called from `TesseractOCRParser.java:305` on every parse), so `Latin` and `HanS` throw `TikaConfigException`. Debian packages install `Latin.traineddata` at the tessdata root, so `-l script/Latin` needs a `tessdata/script/` symlink directory (CI step in Task 5).
2. **OSD says `Korean`, not `Hangul`.** Measured on the Hangul fixture: `Script: Korean`. The mapping keeps `Hangul` and adds `Korean`, both to `script/Hangul`.
3. **Tess4J already writes `tess:script`.** `Tess4JOCRParser.java:473-474` records it today, covered by `Tess4JOCRParserTest.test_psm0`. Spec test 7 already exists; no new code for it.
4. **Tess4J: a fresh `Tesseract` per parse, not a per-model cache.** Tess4J re-creates the native handle on every call anyway (`init()` in `createDocumentsWithResults` and `getOSD`, Tess4J `Tesseract.java:794,856,922`), so a cache saves nothing. A fresh instance also removes the shared `tesseract` field that concurrent parse threads overwrite today (`Tess4JOCRParser.java:396`).
5. **Tess4J installed models come from `TessBaseAPIGetAvailableLanguagesAsVector`,** not a directory listing: with the default datapath `""` (`Tess4JOCRParser.java:350`) Java does not know the tessdata directory.
6. **`skipConfidence` is read from the per-call config,** so the router can force it off. Today it is read from the parser default only (`Tess4JOCRParser.java:333`). Extract never sets it, so this changes nothing for existing callers.
7. **`ocrCache` lines unchanged.** `ImageIOTranscodingOCRParser` already gets the router, so its cached results are routed. Standard images are not cached today either (see Suggested follow-ups).

## Review Focus

1. **Images with too little text for OSD** (stamps, a two-word label like `simple.tiff`): `tesseract --psm 0` exits 1 and Tess4J returns an empty `OSDResult`. Expected: read as `script/Latin`, text kept, no `ocr:script`, no per-image WARN. Pinned by Task 4 `test_a_failed_detection_reads_as_latin_without_a_script`, Task 5 `test_too_little_text_for_detection_reads_as_latin`, Task 2 `test_osd_on_an_image_without_text_is_quiet`.
2. **Images with no text at all** (photos, blank scans): zero words. Expected: empty text, `ocr:confidence` 0.0 (not NaN), no exception. Pinned by Task 1 `test_a_page_without_words_has_no_text_and_zero_confidence`, Task 4 `test_an_image_without_text_keeps_an_empty_first_pass`.
3. **Caller OCR settings surviving per-pass configs** (the Extractor's 12 h `ocrTimeout` against Tika's 120 s default). Expected: every pass sees the caller's timeout, and the caller's config is back in the context after success or failure. Pinned by Task 4 `test_caller_settings_reach_every_pass`, `test_caller_config_is_restored_after_a_failed_read`.
4. **Turning OCR off or picking a language after construction** (`disableOcr()`, `setOcrLanguage()`). Expected: no OCR, or plain OCR with no `ocr:model`. Pinned by existing `ExtractorTest.testDisableOcr` and Task 5 `test_setting_a_language_after_construction_skips_routing`.
5. **Multi-page TIFF in auto mode on `TESSERACT`** (hOCR with several `ocr_page`). Expected: text from every page. Pinned by Task 5 `test_every_page_of_a_multipage_tiff_is_read`.

---

## Before Task 1: environment (about 10 minutes, mostly the leptonica build)

Already done in this container on 2026-10-01 while planning: `tessdata/script/` symlinks, `/usr/lib/aarch64-linux-gnu/libtesseract.so` symlink, `fonts-noto-core fonts-noto-cjk libmagickcore-6.q16-7-extra`.

- [ ] **Step 1: Create the branch**

```bash
git checkout -b feat/ocr-auto-language
```

- [ ] **Step 2: Check the script models are visible under `script/`**

Run: `tesseract --list-langs 2>&1 | grep -E '^(osd|script/)'`
Expected: 7 lines: `osd`, `script/Arabic`, `script/Cyrillic`, `script/HanS`, `script/Hangul`, `script/Japanese`, `script/Latin`.

If missing:

```bash
T=$(tesseract --list-langs 2>&1 | sed -n 's/.*"\(.*\)".*/\1/p')
sudo mkdir -p "$T/script"
for m in Latin HanS Cyrillic Arabic Japanese Hangul; do sudo ln -sf "../$m.traineddata" "$T/script/$m.traineddata"; done
```

- [ ] **Step 3: Build leptonica 1.85 for Tess4J (background)**

The system leptonica is 1.82; Tess4J 5.15 needs `pixBackgroundNormTo1MinMax` (fails today with `UnsatisfiedLinkError`). Same build as `.circleci/config.yml:30-36`:

```bash
cd /tmp && wget -q https://github.com/DanBloomberg/leptonica/releases/download/1.85.0/leptonica-1.85.0.tar.gz && tar zxf leptonica-1.85.0.tar.gz && cd leptonica-1.85.0 && ./configure > /tmp/lept.log 2>&1 && make -j4 >> /tmp/lept.log 2>&1 && sudo make install >> /tmp/lept.log 2>&1; echo "exit=$?"
```

Run it with `run_in_background`, poll `/tmp/lept.log`.

- [ ] **Step 4: Check Tess4J runs**

Run (background): `cd extract-lib && LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH mvn -q -o test -Dtest=Tess4JOCRParserTest > /tmp/t4j.log 2>&1; echo "exit=$?"`
Expected: `exit=0`. Every Maven command below uses the same `LD_LIBRARY_PATH` prefix and `-o`.

---

### Task 1: hOCR text and confidence handler (about 20 minutes)

**Files:**
- Create: `extract-lib/src/main/java/org/icij/extract/ocr/HocrTextHandler.java`
- Test: `extract-lib/src/test/java/org/icij/extract/ocr/HocrTextHandlerTest.java`

**Interfaces:**
- Consumes: nothing.
- Produces: package-private `class HocrTextHandler extends org.xml.sax.helpers.DefaultHandler` with `String text()` (one line per hOCR line, a blank line between `ocr_par`, words joined by one space) and `double meanConfidence()` (mean `x_wconf`, 0 to 100, `0` when there are no words).

- [ ] **Step 1: Write the failing test**

```java
package org.icij.extract.ocr;

import org.junit.Test;

import javax.xml.parsers.SAXParserFactory;
import java.io.ByteArrayInputStream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.fest.assertions.Assertions.assertThat;

public class HocrTextHandlerTest {
    @Test
    public void test_rebuilds_lines_and_paragraphs_and_averages_word_confidence() throws Exception {
        // Given
        String hocr = """
            <html xmlns="http://www.w3.org/1999/xhtml"><body>
             <div class='ocr_page' title='bbox 0 0 100 100'>
              <p class='ocr_par'>
               <span class='ocr_line'><span class='ocrx_word' title='bbox 0 0 1 1; x_wconf 90'>Hello</span> <span class='ocrx_word' title='bbox 0 0 1 1; x_wconf 80'>world</span></span>
               <span class='ocr_line'><span class='ocrx_word' title='bbox 0 0 1 1; x_wconf 70'>again</span></span>
              </p>
              <p class='ocr_par'>
               <span class='ocr_header'><span class='ocrx_word' title='bbox 0 0 1 1; x_wconf 60'><strong>Title</strong></span></span>
              </p>
             </div>
            </body></html>
            """;
        HocrTextHandler handler = new HocrTextHandler();
        // When
        parse(hocr, handler);
        // Then
        assertThat(handler.text()).isEqualTo("Hello world\nagain\n\nTitle");
        assertThat(handler.meanConfidence()).isEqualTo(75.0);
    }

    @Test
    public void test_a_page_without_words_has_no_text_and_zero_confidence() throws Exception {
        // Given
        String hocr = "<html xmlns=\"http://www.w3.org/1999/xhtml\"><body>" +
                "<div class='ocr_page' title='bbox 0 0 100 100'></div></body></html>";
        HocrTextHandler handler = new HocrTextHandler();
        // When
        parse(hocr, handler);
        // Then
        assertThat(handler.text()).isEmpty();
        assertThat(handler.meanConfidence()).isEqualTo(0.0);
    }

    private static void parse(String hocr, HocrTextHandler handler) throws Exception {
        SAXParserFactory factory = SAXParserFactory.newInstance();
        factory.setNamespaceAware(true);
        factory.newSAXParser().parse(new ByteArrayInputStream(hocr.getBytes(UTF_8)), handler);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run (background): `cd extract-lib && LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH mvn -q -o test -Dtest=HocrTextHandlerTest > /tmp/t1.log 2>&1; echo "exit=$?"`
Expected: FAIL, compilation error `cannot find symbol: class HocrTextHandler`. Show the reviewer the error lines.

- [ ] **Step 3: Write the implementation**

```java
package org.icij.extract.ocr;

import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;

import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class HocrTextHandler extends DefaultHandler {
    private static final Pattern WORD_CONFIDENCE = Pattern.compile("x_wconf (\\d+)");
    // Tesseract classes a text line by its layout role, not always as ocr_line.
    private static final Set<String> LINES = Set.of("ocr_line", "ocr_header", "ocr_caption", "ocr_textfloat");
    private static final String PARAGRAPH_BREAK = "\n\n";

    private final StringBuilder text = new StringBuilder();
    private final StringBuilder word = new StringBuilder();
    private String separator = "";
    private int depth = 0;
    private int wordDepth = -1;
    private long confidenceSum = 0;
    private int confidenceCount = 0;

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) {
        depth++;
        String hocrClass = attributes.getValue("class");
        if ("ocr_par".equals(hocrClass)) {
            separator = PARAGRAPH_BREAK;
        } else if (LINES.contains(hocrClass) && !separator.equals(PARAGRAPH_BREAK)) {
            separator = "\n";
        } else if ("ocrx_word".equals(hocrClass)) {
            wordDepth = depth;
            addConfidence(attributes.getValue("title"));
        }
    }

    @Override
    public void endElement(String uri, String localName, String qName) {
        if (depth == wordDepth) {
            appendWord();
            wordDepth = -1;
        }
        depth--;
    }

    @Override
    public void characters(char[] ch, int start, int length) {
        if (wordDepth != -1) {
            word.append(ch, start, length);
        }
    }

    String text() {
        return text.toString();
    }

    double meanConfidence() {
        return confidenceCount == 0 ? 0 : (double) confidenceSum / confidenceCount;
    }

    private void addConfidence(String title) {
        Matcher matcher = WORD_CONFIDENCE.matcher(Objects.toString(title, ""));
        if (matcher.find()) {
            confidenceSum += Integer.parseInt(matcher.group(1));
            confidenceCount++;
        }
    }

    private void appendWord() {
        String value = word.toString().strip();
        word.setLength(0);
        if (value.isEmpty()) {
            return;
        }
        if (!text.isEmpty()) {
            text.append(separator);
        }
        text.append(value);
        separator = " ";
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: same command as Step 2. Expected: `exit=0`.

- [ ] **Step 5: Commit**

Use `/commit` with `src/main/java/org/icij/extract/ocr/HocrTextHandler.java` and its test. Message: `feat(ocr): read plain text and mean word confidence from hOCR`.

---

### Task 2: Fixtures and Tess4J per-call language (about 1 hour)

**Files:**
- Create: `extract-lib/src/test/resources/documents/ocr/auto/{latin,cyrillic,arabic,hans,japanese,hangul,mixed}.png`
- Modify: `extract-lib/src/main/java/org/icij/extract/ocr/Tess4JOCRParser.java` (fields `:129-130`, `getOrInit` `:337-366`, private `parse` `:390-418`, `buildOCRRunner` `:453-467`, `extractOSDOutput` `:469-475`, runners `:550-651`)
- Test: `extract-lib/src/test/java/org/icij/extract/ocr/Tess4JOCRParserTest.java`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - Fixtures under `/documents/ocr/auto/`.
  - `public Set<String> Tess4JOCRParser.installedModels()`: models tesseract sees with this parser's datapath, e.g. `osd`, `script/Latin`. Empty set when `osd` cannot load.
  - `Tess4JOCRParser` reads the language, PSM, and `skipConfidence` from the per-call config (`defaultConfig.cloneAndUpdate(contextConfig)`) on every parse.

- [ ] **Step 1: Render the fixtures**

Text written from scratch. `pango:` shapes Arabic and picks fonts per script.

```bash
mkdir -p extract-lib/src/test/resources/documents/ocr/auto && cd $_ && bash <<'EOF'
render() { convert -background white -density 200 pango:"<span font=\"$2 12\">$3</span>" -bordercolor white -border 60 "$1.png"; }
render latin "Noto Sans" "The city council published its annual budget report on Monday morning.
Residents can read the full document at the public library until the end of June."
render cyrillic "Noto Sans" "Городской совет опубликовал годовой отчёт о бюджете в понедельник утром.
Жители могут прочитать полный документ в публичной библиотеке до конца июня."
render arabic "Noto Sans Arabic" "نشر مجلس المدينة تقرير الميزانية السنوي صباح يوم الاثنين.
يمكن للسكان قراءة الوثيقة كاملة في المكتبة العامة حتى نهاية شهر يونيو."
render hans "Noto Sans CJK SC" "市议会在星期一上午发布了年度预算报告。
居民可以在公共图书馆阅读完整的文件，直到六月底。"
render japanese "Noto Sans CJK JP" "市議会は月曜日の朝に年間予算の報告書を発表しました。
住民は六月末まで公共図書館で文書の全文を読むことができます。"
render hangul "Noto Sans CJK KR" "시의회는 월요일 아침에 연간 예산 보고서를 발표했습니다.
주민들은 유월 말까지 공공 도서관에서 전체 문서를 읽을 수 있습니다."
render mixed "Noto Sans CJK SC" "市议会在星期一上午发布了年度预算报告。
Городской совет опубликовал годовой отчёт о бюджете.
The city council published its annual budget report."
EOF
ls -la *.png
```

- [ ] **Step 2: Check OSD on every fixture**

```bash
cd extract-lib/src/test/resources/documents/ocr/auto && for f in *.png; do printf '%s: ' "$f"; tesseract "$f" - --psm 0 2>/dev/null | grep '^Script:'; done
```

Expected (measured while planning): `arabic.png: Script: Arabic`, `cyrillic.png: Script: Cyrillic`, `hangul.png: Script: Korean`, `hans.png: Script: Han`, `japanese.png: Script: Japanese`, `latin.png: Script: Latin`, `mixed.png: Script: Latin`. If any differs, stop and report to the maintainer: Task 5 expectations depend on it.

- [ ] **Step 3: Write the failing tests**

Add to `Tess4JOCRParserTest` (imports: `ch.qos.logback.classic.Level`, `ch.qos.logback.classic.Logger`, `ch.qos.logback.classic.spi.ILoggingEvent`, `ch.qos.logback.core.read.ListAppender`, `org.slf4j.LoggerFactory`, `java.awt.Color`, `java.awt.Graphics2D`, `java.awt.image.BufferedImage`, `javax.imageio.ImageIO`, `java.io.ByteArrayInputStream`, `java.io.ByteArrayOutputStream`, `java.io.IOException`):

```java
    @Test
    public void test_text_output_with_confidence() throws Exception {
        // Given
        Metadata metadata = getMetadata(MediaType.image("png"));
        // When
        String text = parseWith(new Tess4JOCRParser(), "test.png", new TesseractOCRConfig(), metadata);
        // Then
        assertThat(text).contains("The (quick) [brown] {fox} jumps!");
        assertThat(metadata.get(OCR_CONFIDENCE)).isNotNull();
    }

    @Test
    public void test_each_parse_uses_its_own_language() throws Exception {
        // Given
        Tess4JOCRParser parser = new Tess4JOCRParser();
        TesseractOCRConfig english = new TesseractOCRConfig();
        english.setLanguage("eng");
        TesseractOCRConfig chinese = new TesseractOCRConfig();
        chinese.setLanguage("script/HanS+script/Latin");
        parseWith(parser, "auto/hans.png", english, getMetadata(MediaType.image("png")));
        // When
        String text = parseWith(parser, "auto/hans.png", chinese, getMetadata(MediaType.image("png")));
        // Then
        assertThat(text.replaceAll("\\s+", "")).contains("预算报告");
    }

    @Test
    public void test_per_call_config_turns_confidence_back_on() throws Exception {
        // Given
        Tess4JOCRParser parser = new Tess4JOCRParser();
        parser.setSkipConfidence(true);
        TesseractOCRConfig config = new TesseractOCRConfig();
        config.addOtherTesseractConfig(Tess4JOCRParser.SKIP_CONFIDENCE, "false");
        Metadata metadata = getMetadata(MediaType.image("png"));
        // When
        parseWith(parser, "test.png", config, metadata);
        // Then
        assertThat(metadata.get(OCR_CONFIDENCE)).isNotNull();
    }

    @Test
    public void test_osd_on_an_image_without_text_is_quiet() throws Exception {
        // Given
        Logger log = (Logger) LoggerFactory.getLogger(Tess4JOCRParser.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        log.addAppender(appender);
        TesseractOCRConfig config = new TesseractOCRConfig();
        config.setPageSegMode("0");
        Metadata metadata = getMetadata(MediaType.image("png"));
        // When
        try {
            parseWith(new Tess4JOCRParser(), blankPng(), config, metadata);
        } finally {
            log.detachAppender(appender);
        }
        // Then
        assertThat(metadata.get(PSM0_SCRIPT)).isNull();
        assertThat(appender.list.stream().filter(e -> e.getLevel().isGreaterOrEqual(Level.WARN)).toList()).isEmpty();
    }

    private static String parseWith(Parser parser, String path, TesseractOCRConfig config, Metadata metadata) throws Exception {
        return parseWith(parser, Tess4JOCRParserTest.class.getResourceAsStream(SAMPLE_DOCS_PATH_PREFIX + "/" + path), config, metadata);
    }

    private static String parseWith(Parser parser, InputStream image, TesseractOCRConfig config, Metadata metadata) throws Exception {
        ParseContext context = new ParseContext();
        context.set(TesseractOCRConfig.class, config);
        BodyContentHandler handler = new BodyContentHandler(-1);
        try (image) {
            parser.parse(image, handler, metadata, context);
        }
        return handler.toString();
    }

    private static InputStream blankPng() throws IOException {
        BufferedImage image = new BufferedImage(300, 100, BufferedImage.TYPE_BYTE_GRAY);
        Graphics2D graphics = image.createGraphics();
        graphics.setColor(Color.WHITE);
        graphics.fillRect(0, 0, 300, 100);
        graphics.dispose();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ImageIO.write(image, "png", out);
        return new ByteArrayInputStream(out.toByteArray());
    }
```

- [ ] **Step 4: Run tests to verify they fail**

Run (background): `cd extract-lib && LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH mvn -q -o test -Dtest=Tess4JOCRParserTest > /tmp/t2.log 2>&1; echo "exit=$?"`
Expected: FAIL. Show the reviewer each runtime failure:
- `test_text_output_with_confidence`: text empty. Suspected cause: `OCRRunner` reads the base output file, but Tess4J's text renderer writes `<base>.txt`. **If this test passes, the bug is not real: report it and skip the `.txt` change in Step 5.**
- `test_each_parse_uses_its_own_language`: no Chinese, the second parse reuses the cached `eng` instance.
- `test_per_call_config_turns_confidence_back_on`: `ocr:confidence` null.
- `test_osd_on_an_image_without_text_is_quiet`: one WARN with a `NullPointerException` from `Optional.of(osd.getScriptName())`. **If it passes, report it and skip the `ofNullable` change.**

`Tess4JOCRParser.installedModels()` gets its test in Task 3 (`OCRConfigAdapterTest.test_tess4j_lists_osd_and_script_models`), where it goes RED first.

- [ ] **Step 5: Implement**

In `Tess4JOCRParser.java`, add import `com.sun.jna.ptr.PointerByReference`. Delete the fields at `:129-130`:

```java
    private volatile ITesseract tesseract;
    private List<ITesseract.RenderedFormat> supportedFormats;
```

Replace `getOrInit` (`:337-366`) with:

```java
    public Set<String> installedModels() {
        TessAPI api = TessAPI.INSTANCE;
        ITessAPI.TessBaseAPI handle = api.TessBaseAPICreate();
        try {
            if (api.TessBaseAPIInit3(handle, datapath(), "osd") != 0) {
                return Set.of();
            }
            PointerByReference models = api.TessBaseAPIGetAvailableLanguagesAsVector(handle);
            try {
                return Set.copyOf(Arrays.asList(models.getPointer().getStringArray(0)));
            } finally {
                api.TessDeleteTextArray(models);
            }
        } finally {
            api.TessBaseAPIEnd(handle);
            api.TessBaseAPIDelete(handle);
        }
    }

    private String datapath() {
        if (getTessdataPath().isEmpty()) {
            return getTessdataPath();
        }
        // Ugly hack, we have to reload the lib... Sadly Tess4J doesn't let us override the lib
        // loading easily and it doesn't let
        Library.Handler handler = new Library.Handler(getTesseractLibName(), TessAPI.class, Map.of());
        return handler.getNativeLibrary().getFile().getPath();
    }

    private static ITesseract newTesseract(String datapath, TesseractOCRConfig config) {
        Tesseract tesseract = new Tesseract();
        tesseract.setDatapath(datapath);
        tesseract.setVariable("debug_file", (isWindows()) ? "NUL" : "/dev/null");
        tesseract.setLanguage(config.getLanguage());
        tesseract.setPageSegMode(Integer.parseInt(config.getPageSegMode()));
        tesseract.setVariable("page_separator", config.getPageSeparator());
        tesseract.setVariable("preserve_interword_spaces", config.isPreserveInterwordSpacing() ? "1" : "0");
        return tesseract;
    }
```

In the private `parse(...)` (`:390-418`), replace `tesseract = getOrInit(config);` with `ITesseract tesseract = newTesseract(datapath(), config);` and pass it on:

```java
            Callable<?> ocrRunner = buildOCRRunner(
                tesseract, bufferedImage, stream.getFile(), processedImageFile, config, metadata, tmp, xhtml
            );
```

Replace `buildOCRRunner` (`:453-467`):

```java
    private Callable<?> buildOCRRunner(
        ITesseract tesseract, BufferedImage bufferedImage, File rawImageFile, File processedImageFile,
        TesseractOCRConfig config, Metadata metadata, TemporaryResources tmp, XHTMLContentHandler xhtml
    ) throws IOException {
        if (config.getPageSegMode().equals("0")) {
            return new OSDRunner(tesseract, bufferedImage, processedImageFile, metadata);
        }
        boolean skipConfidence = Boolean.parseBoolean(config.getOtherTesseractConfig().get(SKIP_CONFIDENCE));
        if (skipConfidence && config.getOutputType().equals(TesseractOCRConfig.OUTPUT_TYPE.TXT)) {
            return new TextOCRRunner(tesseract, bufferedImage, processedImageFile, xhtml);
        }
        if (processedImageFile != null) {
            return new OCRRunner(tesseract, null, processedImageFile, config, metadata, xhtml, tmp);
        }
        return new OCRRunner(tesseract, bufferedImage, rawImageFile, config, metadata, xhtml, tmp);
    }
```

In `extractOSDOutput` (`:473`), use `Optional.ofNullable(osd.getScriptName())`.

In each runner (`OSDRunner`, `TextOCRRunner`, `OCRRunner`), add a first constructor parameter `ITesseract tesseract` stored in a `private final ITesseract tesseract;` field. In `OCRRunner.call()`, replace both `supportedFormats` arguments with a local:

```java
                List<ITesseract.RenderedFormat> formats = List.of(Objects.requireNonNull(getRenderedFormat(config)));
```

and replace the hOCR-only rename:

```java
                if (isHOCR) {
                    outputFile = new File(outputFile.toPath().resolveSibling(outputFile.getName() + ".hocr").toUri());
                }
```

with:

```java
                String extension = isHOCR ? ".hocr" : ".txt";
                outputFile = new File(outputFile.toPath().resolveSibling(outputFile.getName() + extension).toUri());
```

- [ ] **Step 6: Run tests to verify they pass**

Run: same command as Step 4. Expected: `exit=0`, all of `Tess4JOCRParserTest` green (the existing tests too).

- [ ] **Step 7: Commit**

Use `/commit` with the 7 fixtures, `Tess4JOCRParser.java` and `Tess4JOCRParserTest.java`. Message: `fix(ocr): read each Tess4J parse with its own language and keep its text`.

---

### Task 3: Installed models on both OCR backends (about 20 minutes)

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/extract/ocr/OCRConfigAdapter.java`
- Modify: `extract-lib/src/main/java/org/icij/extract/ocr/TesseractOCRConfigAdapter.java`
- Modify: `extract-lib/src/main/java/org/icij/extract/ocr/Tess4JOCRConfigAdapter.java`
- Test: `extract-lib/src/test/java/org/icij/extract/ocr/OCRConfigAdapterTest.java`

**Interfaces:**
- Consumes: `Tess4JOCRParser.installedModels()` (Task 2).
- Produces: `Set<String> OCRConfigAdapter.installedModels()`. Empty set when tesseract or its native library is unavailable. Never throws.

- [ ] **Step 1: Write the failing test**

```java
package org.icij.extract.ocr;

import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

public class OCRConfigAdapterTest {
    @Test
    public void test_tesseract_lists_osd_and_script_models() {
        assertThat(new TesseractOCRConfigAdapter().installedModels()).contains("osd", "script/Latin", "script/HanS");
    }

    @Test
    public void test_tess4j_lists_osd_and_script_models() {
        assertThat(new Tess4JOCRConfigAdapter().installedModels()).contains("osd", "script/Latin", "script/HanS");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run (background): `cd extract-lib && LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH mvn -q -o test -Dtest=OCRConfigAdapterTest > /tmp/t3.log 2>&1; echo "exit=$?"`
Expected: compilation error `cannot find symbol: method installedModels()`.

- [ ] **Step 3: Implement**

`OCRConfigAdapter.java`: add `import java.util.Set;` and

```java
    Set<String> installedModels();
```

`TesseractOCRConfigAdapter.java`: add `import java.util.Set;` and

```java
    @Override
    public Set<String> installedModels() {
        try {
            TesseractOCRParser probe = new TesseractOCRParser();
            probe.setPreloadLangs(true);
            probe.initialize(new HashMap<>());
            return probe.getLangs();
        } catch (TikaConfigException e) {
            return Set.of();
        }
    }
```

(`preloadLangs` runs `tesseract --list-langs`, `TesseractOCRParser.java:886`; without it `getLangs()` stays empty.)

`Tess4JOCRConfigAdapter.java`: add `import java.util.Set;` and

```java
    @Override
    public Set<String> installedModels() {
        try {
            return new Tess4JOCRParser().installedModels();
        } catch (LinkageError e) {
            return Set.of();
        }
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: same as Step 2. Expected: `exit=0`.

- [ ] **Step 5: Commit**

Use `/commit` with the three adapters and the test. Message: `feat(ocr): list the tesseract models each OCR backend can load`.

---

### Task 4: The router (about 1.5 hours)

**Files:**
- Create: `extract-lib/src/main/java/org/icij/extract/ocr/AutoLanguageOCRParser.java`
- Test: `extract-lib/src/test/java/org/icij/extract/ocr/AutoLanguageOCRParserTest.java`

**Interfaces:**
- Consumes: `HocrTextHandler` (Task 1), `Tess4JOCRParser.SKIP_CONFIDENCE` (`"skipConfidence"`), `ParserWithConfidence.OCR_CONFIDENCE` (`ocr:confidence`), `TesseractOCRParser.PSM0_SCRIPT` / `PSM0_SCRIPT_CONFIDENCE` (`tess:script`, `tess:script_confidence`).
- Produces:
  - `public AutoLanguageOCRParser(Parser delegate, boolean readsHocr, int retryConfidence, BooleanSupplier enabled)`. `readsHocr` is true for `TESSERACT`. `enabled` false means pass straight through.
  - `public static final Property OCR_SCRIPT` (`ocr:script`), `OCR_SCRIPT_CONFIDENCE` (`ocr:script_confidence`), `OCR_MODEL` (`ocr:model`).
  - `public static Set<String> missingModels(Set<String> installed)`: required models not in `installed`, sorted.

- [ ] **Step 1: Write the failing tests**

```java
package org.icij.extract.ocr;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.ocr.TesseractOCRConfig;
import org.apache.tika.parser.ocr.TesseractOCRParser;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.XHTMLContentHandler;
import org.fest.assertions.Delta;
import org.junit.Test;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.fest.assertions.Assertions.assertThat;
import static org.icij.extract.ocr.AutoLanguageOCRParser.OCR_MODEL;
import static org.icij.extract.ocr.AutoLanguageOCRParser.OCR_SCRIPT;
import static org.icij.extract.ocr.AutoLanguageOCRParser.OCR_SCRIPT_CONFIDENCE;
import static org.icij.extract.ocr.ParserWithConfidence.OCR_CONFIDENCE;
import static org.junit.Assert.assertThrows;

public class AutoLanguageOCRParserTest {
    private static final String ALL = "script/Latin+script/HanS+script/Cyrillic+script/Arabic+script/Japanese+script/Hangul";

    @Test
    public void test_routes_han_to_simplified_chinese_plus_latin() throws Exception {
        // Given
        StubOcr stub = new StubOcr();
        Metadata metadata = new Metadata();
        // When
        String text = parse(router(stub, 60), metadata, new ParseContext());
        // Then
        assertThat(stub.calls).isEqualTo(List.of("osd", "script/HanS+script/Latin"));
        assertThat(text).isEqualTo("read with script/HanS+script/Latin");
        assertThat(metadata.get(OCR_SCRIPT)).isEqualTo("Han");
        assertThat(Double.parseDouble(metadata.get(OCR_SCRIPT_CONFIDENCE))).isEqualTo(0.76, Delta.delta(0.001));
        assertThat(metadata.get(OCR_MODEL)).isEqualTo("script/HanS+script/Latin");
        assertThat(Double.parseDouble(metadata.get(OCR_CONFIDENCE))).isEqualTo(0.9, Delta.delta(0.001));
    }

    @Test
    public void test_maps_each_osd_script_to_its_first_pass_model() throws Exception {
        // Given
        Map<String, String> expected = Map.of(
                "Latin", "script/Latin",
                "Cyrillic", "script/Cyrillic+script/Latin",
                "Arabic", "script/Arabic+script/Latin",
                "Hangul", "script/Hangul+script/Latin",
                "Korean", "script/Hangul+script/Latin",
                "Japanese", "script/Japanese+script/Latin",
                "Hiragana", "script/Japanese+script/Latin",
                "Katakana", "script/Japanese+script/Latin");
        for (Map.Entry<String, String> entry : expected.entrySet()) {
            StubOcr stub = new StubOcr();
            stub.script = entry.getKey();
            Metadata metadata = new Metadata();
            // When
            parse(router(stub, 60), metadata, new ParseContext());
            // Then
            assertThat(metadata.get(OCR_MODEL)).as(entry.getKey()).isEqualTo(entry.getValue());
        }
    }

    @Test
    public void test_an_unknown_script_reads_once_with_every_script() throws Exception {
        // Given
        StubOcr stub = new StubOcr();
        stub.script = "Devanagari";
        stub.confidence.put(ALL, 0.3);
        Metadata metadata = new Metadata();
        // When
        parse(router(stub, 60), metadata, new ParseContext());
        // Then
        assertThat(stub.calls).isEqualTo(List.of("osd", ALL));
        assertThat(metadata.get(OCR_SCRIPT)).isEqualTo("Devanagari");
        assertThat(metadata.get(OCR_MODEL)).isEqualTo(ALL);
    }

    @Test
    public void test_a_failed_detection_reads_as_latin_without_a_script() throws Exception {
        // Given
        StubOcr stub = new StubOcr();
        stub.script = null;
        Metadata metadata = new Metadata();
        // When
        parse(router(stub, 60), metadata, new ParseContext());
        // Then
        assertThat(stub.calls).isEqualTo(List.of("osd", "script/Latin"));
        assertThat(metadata.get(OCR_SCRIPT)).isNull();
        assertThat(metadata.get(OCR_SCRIPT_CONFIDENCE)).isNull();
        assertThat(metadata.get(OCR_MODEL)).isEqualTo("script/Latin");
    }

    @Test
    public void test_a_low_first_pass_retries_and_keeps_the_better_read() throws Exception {
        // Given
        StubOcr stub = new StubOcr();
        stub.confidence.put("script/HanS+script/Latin", 0.3);
        stub.confidence.put(ALL, 0.8);
        Metadata metadata = new Metadata();
        // When
        String text = parse(router(stub, 60), metadata, new ParseContext());
        // Then
        assertThat(stub.calls).isEqualTo(List.of("osd", "script/HanS+script/Latin", ALL));
        assertThat(text).isEqualTo("read with " + ALL);
        assertThat(metadata.get(OCR_MODEL)).isEqualTo(ALL);
        assertThat(Double.parseDouble(metadata.get(OCR_CONFIDENCE))).isEqualTo(0.8, Delta.delta(0.001));
    }

    @Test
    public void test_a_worse_retry_keeps_the_first_pass() throws Exception {
        // Given
        StubOcr stub = new StubOcr();
        stub.confidence.put("script/HanS+script/Latin", 0.3);
        stub.confidence.put(ALL, 0.2);
        Metadata metadata = new Metadata();
        // When
        String text = parse(router(stub, 60), metadata, new ParseContext());
        // Then
        assertThat(text).isEqualTo("read with script/HanS+script/Latin");
        assertThat(metadata.get(OCR_MODEL)).isEqualTo("script/HanS+script/Latin");
        assertThat(Double.parseDouble(metadata.get(OCR_CONFIDENCE))).isEqualTo(0.3, Delta.delta(0.001));
    }

    @Test
    public void test_retry_confidence_zero_never_retries() throws Exception {
        // Given
        StubOcr stub = new StubOcr();
        stub.confidence.put("script/HanS+script/Latin", 0.3);
        // When
        parse(router(stub, 0), new Metadata(), new ParseContext());
        // Then
        assertThat(stub.calls).isEqualTo(List.of("osd", "script/HanS+script/Latin"));
    }

    @Test
    public void test_an_image_without_text_keeps_an_empty_first_pass() throws Exception {
        // Given
        StubOcr stub = new StubOcr();
        stub.script = null;
        stub.text = "";
        stub.confidence.put("script/Latin", 0.0);
        stub.confidence.put(ALL, 0.0);
        Metadata metadata = new Metadata();
        // When
        String text = parse(router(stub, 60), metadata, new ParseContext());
        // Then
        assertThat(text).isEmpty();
        assertThat(stub.calls).isEqualTo(List.of("osd", "script/Latin", ALL));
        assertThat(metadata.get(OCR_MODEL)).isEqualTo("script/Latin");
        assertThat(Double.parseDouble(metadata.get(OCR_CONFIDENCE))).isEqualTo(0.0);
    }

    @Test
    public void test_detection_metadata_does_not_reach_the_document() throws Exception {
        // Given
        Metadata metadata = new Metadata();
        // When
        parse(router(new StubOcr(), 60), metadata, new ParseContext());
        // Then
        assertThat(metadata.get(TesseractOCRParser.PSM0_SCRIPT)).isNull();
        assertThat(metadata.get(TesseractOCRParser.PSM0_SCRIPT_CONFIDENCE)).isNull();
    }

    @Test
    public void test_caller_settings_reach_every_pass() throws Exception {
        // Given
        StubOcr stub = new StubOcr();
        stub.confidence.put("script/HanS+script/Latin", 0.3);
        TesseractOCRConfig caller = new TesseractOCRConfig();
        caller.setTimeoutSeconds(42);
        ParseContext context = new ParseContext();
        context.set(TesseractOCRConfig.class, caller);
        // When
        parse(router(stub, 60), new Metadata(), context);
        // Then
        assertThat(stub.timeouts).isEqualTo(List.of(42, 42, 42));
        assertThat(context.get(TesseractOCRConfig.class)).isSameAs(caller);
    }

    @Test
    public void test_caller_config_is_restored_after_a_failed_read() {
        // Given
        StubOcr stub = new StubOcr();
        stub.failOcr = true;
        TesseractOCRConfig caller = new TesseractOCRConfig();
        ParseContext context = new ParseContext();
        context.set(TesseractOCRConfig.class, caller);
        // When
        TikaException error = assertThrows(TikaException.class, () -> parse(router(stub, 60), new Metadata(), context));
        // Then
        assertThat(error.getMessage()).isEqualTo("OCR timeout");
        assertThat(context.get(TesseractOCRConfig.class)).isSameAs(caller);
    }

    @Test
    public void test_a_disabled_router_passes_straight_through() throws Exception {
        // Given
        StubOcr stub = new StubOcr();
        Metadata metadata = new Metadata();
        AutoLanguageOCRParser router = new AutoLanguageOCRParser(stub, false, 60, () -> false);
        // When
        String text = parse(router, metadata, new ParseContext());
        // Then
        assertThat(stub.calls).isEqualTo(List.of("eng"));
        assertThat(text).isEqualTo("read with eng");
        assertThat(metadata.get(OCR_MODEL)).isNull();
    }

    @Test
    public void test_missing_models_lists_what_is_not_installed() {
        assertThat(AutoLanguageOCRParser.missingModels(Set.of("eng", "osd", "script/Latin")))
                .containsOnly("script/HanS", "script/Cyrillic", "script/Arabic", "script/Japanese", "script/Hangul");
        assertThat(AutoLanguageOCRParser.missingModels(Set.of("osd", "script/Latin", "script/HanS",
                "script/Cyrillic", "script/Arabic", "script/Japanese", "script/Hangul"))).isEmpty();
    }

    private static AutoLanguageOCRParser router(StubOcr stub, int retryConfidence) {
        return new AutoLanguageOCRParser(stub, false, retryConfidence, () -> true);
    }

    private static String parse(Parser parser, Metadata metadata, ParseContext context) throws Exception {
        BodyContentHandler handler = new BodyContentHandler(-1);
        parser.parse(new ByteArrayInputStream(new byte[] {1, 2, 3}), handler, metadata, context);
        return handler.toString().strip();
    }

    private static class StubOcr implements Parser {
        final List<String> calls = new ArrayList<>();
        final List<Integer> timeouts = new ArrayList<>();
        final Map<String, Double> confidence = new HashMap<>();
        String script = "Han";
        String text = null;
        boolean failOcr = false;

        @Override
        public Set<MediaType> getSupportedTypes(ParseContext context) {
            return Set.of(MediaType.image("ocr-png"));
        }

        @Override
        public void parse(InputStream stream, ContentHandler handler, Metadata metadata, ParseContext context)
                throws SAXException, TikaException {
            TesseractOCRConfig config = context.get(TesseractOCRConfig.class, new TesseractOCRConfig());
            timeouts.add(config.getTimeoutSeconds());
            if ("0".equals(config.getPageSegMode())) {
                calls.add("osd");
                if (script == null) {
                    throw new TikaException("Too few characters. Skipping this page");
                }
                metadata.set(TesseractOCRParser.PSM0_SCRIPT, script);
                metadata.set(TesseractOCRParser.PSM0_SCRIPT_CONFIDENCE, 0.76);
                return;
            }
            calls.add(config.getLanguage());
            if (failOcr) {
                throw new TikaException("OCR timeout");
            }
            metadata.set(OCR_CONFIDENCE, confidence.getOrDefault(config.getLanguage(), 0.9));
            XHTMLContentHandler xhtml = new XHTMLContentHandler(handler, metadata);
            xhtml.startDocument();
            xhtml.element("div", text == null ? "read with " + config.getLanguage() : text);
            xhtml.endDocument();
        }
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run (background): `cd extract-lib && LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH mvn -q -o test -Dtest=AutoLanguageOCRParserTest > /tmp/t4.log 2>&1; echo "exit=$?"`
Expected: compilation error `cannot find symbol: class AutoLanguageOCRParser`.

- [ ] **Step 3: Implement**

```java
package org.icij.extract.ocr;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TemporaryResources;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.Property;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.ocr.TesseractOCRConfig;
import org.apache.tika.parser.ocr.TesseractOCRParser;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.XHTMLContentHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BooleanSupplier;

import static org.icij.extract.ocr.ParserWithConfidence.OCR_CONFIDENCE;

/**
 * Picks the tesseract model per image: detects the script with OSD, reads the image with that script's
 * model plus Latin, and reads it again with every supported script when the first read scores below
 * the retry confidence, keeping the better read.
 */
public class AutoLanguageOCRParser implements Parser {
    public static final Property OCR_SCRIPT = Property.externalText("ocr:script");
    public static final Property OCR_SCRIPT_CONFIDENCE = Property.externalReal("ocr:script_confidence");
    public static final Property OCR_MODEL = Property.externalText("ocr:model");

    private static final Logger LOGGER = LoggerFactory.getLogger(AutoLanguageOCRParser.class);
    private static final String LATIN = "script/Latin";
    private static final String ALL_SCRIPTS = String.join("+",
            LATIN, "script/HanS", "script/Cyrillic", "script/Arabic", "script/Japanese", "script/Hangul");
    private static final Set<String> REQUIRED_MODELS = Set.of(
            "osd", LATIN, "script/HanS", "script/Cyrillic", "script/Arabic", "script/Japanese", "script/Hangul");
    // OSD names Hangul text "Korean".
    private static final Map<String, String> FIRST_PASS_MODEL = Map.of(
            "Latin", LATIN,
            "Han", "script/HanS+" + LATIN,
            "Cyrillic", "script/Cyrillic+" + LATIN,
            "Arabic", "script/Arabic+" + LATIN,
            "Hangul", "script/Hangul+" + LATIN,
            "Korean", "script/Hangul+" + LATIN,
            "Japanese", "script/Japanese+" + LATIN,
            "Hiragana", "script/Japanese+" + LATIN,
            "Katakana", "script/Japanese+" + LATIN);

    private final Parser delegate;
    private final boolean readsHocr;
    private final int retryConfidence;
    private final BooleanSupplier enabled;

    public AutoLanguageOCRParser(Parser delegate, boolean readsHocr, int retryConfidence, BooleanSupplier enabled) {
        this.delegate = delegate;
        this.readsHocr = readsHocr;
        this.retryConfidence = retryConfidence;
        this.enabled = enabled;
    }

    public static Set<String> missingModels(Set<String> installed) {
        Set<String> missing = new TreeSet<>(REQUIRED_MODELS);
        missing.removeAll(installed);
        return missing;
    }

    @Override
    public Set<MediaType> getSupportedTypes(ParseContext context) {
        return delegate.getSupportedTypes(context);
    }

    @Override
    public void parse(InputStream stream, ContentHandler handler, Metadata metadata, ParseContext context)
            throws IOException, SAXException, TikaException {
        if (!enabled.getAsBoolean()) {
            delegate.parse(stream, handler, metadata, context);
            return;
        }
        TesseractOCRConfig callerConfig = context.get(TesseractOCRConfig.class);
        TesseractOCRConfig base = callerConfig == null ? new TesseractOCRConfig() : callerConfig;
        try (TemporaryResources tmp = new TemporaryResources()) {
            Path image = TikaInputStream.get(stream, tmp, metadata).getPath();
            Metadata detection = detect(image, base, metadata, context);
            String script = StringUtils.trimToNull(detection.get(TesseractOCRParser.PSM0_SCRIPT));
            String firstModel = script == null ? LATIN : FIRST_PASS_MODEL.getOrDefault(script, ALL_SCRIPTS);
            Pass best = read(image, firstModel, base, metadata, context);
            if (best.confidence() < retryConfidence && !firstModel.equals(ALL_SCRIPTS)) {
                Pass retry = read(image, ALL_SCRIPTS, base, metadata, context);
                if (retry.confidence() > best.confidence()) {
                    best = retry;
                }
            }
            emit(best.text(), handler, metadata);
            if (script != null) {
                metadata.set(OCR_SCRIPT, script);
                Optional.ofNullable(detection.get(TesseractOCRParser.PSM0_SCRIPT_CONFIDENCE))
                        .ifPresent(confidence -> metadata.set(OCR_SCRIPT_CONFIDENCE, Double.parseDouble(confidence)));
            }
            metadata.set(OCR_MODEL, best.model());
            metadata.set(OCR_CONFIDENCE, best.confidence() / 100);
        } finally {
            context.set(TesseractOCRConfig.class, callerConfig);
        }
    }

    private Metadata detect(Path image, TesseractOCRConfig base, Metadata metadata, ParseContext context) {
        TesseractOCRConfig config = SerializationUtils.clone(base);
        config.setPageSegMode("0");
        Metadata scratch = copyOf(metadata);
        try {
            run(image, config, new DefaultHandler(), scratch, context);
            return scratch;
        } catch (IOException | SAXException | TikaException e) {
            LOGGER.debug("script detection failed, reading as {}: {}", LATIN, e.toString());
            return new Metadata();
        }
    }

    private Pass read(Path image, String model, TesseractOCRConfig base, Metadata metadata, ParseContext context)
            throws IOException, SAXException, TikaException {
        TesseractOCRConfig config = SerializationUtils.clone(base);
        config.setLanguage(model);
        metadata.remove(OCR_CONFIDENCE.getName());
        if (readsHocr) {
            config.setOutputType(TesseractOCRConfig.OUTPUT_TYPE.HOCR);
            HocrTextHandler hocr = new HocrTextHandler();
            run(image, config, hocr, metadata, context);
            return new Pass(model, hocr.text(), hocr.meanConfidence());
        }
        config.addOtherTesseractConfig(Tess4JOCRParser.SKIP_CONFIDENCE, "false");
        BodyContentHandler text = new BodyContentHandler(-1);
        run(image, config, text, metadata, context);
        double confidence = Optional.ofNullable(metadata.get(OCR_CONFIDENCE)).map(Double::parseDouble).orElse(0.0);
        return new Pass(model, text.toString().strip(), 100 * confidence);
    }

    private void run(Path image, TesseractOCRConfig config, ContentHandler handler, Metadata metadata,
                     ParseContext context) throws IOException, SAXException, TikaException {
        context.set(TesseractOCRConfig.class, config);
        try (TikaInputStream input = TikaInputStream.get(image)) {
            delegate.parse(input, handler, metadata, context);
        }
    }

    private static void emit(String text, ContentHandler handler, Metadata metadata) throws SAXException {
        XHTMLContentHandler xhtml = new XHTMLContentHandler(handler, metadata);
        xhtml.startDocument();
        xhtml.startElement("div", "class", "ocr");
        xhtml.characters(text);
        xhtml.endElement("div");
        xhtml.endDocument();
    }

    private static Metadata copyOf(Metadata metadata) {
        Metadata copy = new Metadata();
        for (String name : metadata.names()) {
            for (String value : metadata.getValues(name)) {
                copy.add(name, value);
            }
        }
        return copy;
    }

    private record Pass(String model, String text, double confidence) {}
}
```

Why these choices, so a reviewer does not undo them:
- `SerializationUtils.clone` keeps the caller config's private `userConfigured` set. `cloneAndUpdate` drops it (`TesseractOCRConfig.java:549`), and the delegate's `defaultConfig.cloneAndUpdate(contextConfig)` would then fall back to Tika defaults, losing the Extractor's OCR timeout.
- OCR passes write into the real `metadata`, so `OCRParserAdapter` still sets `ocr:parser` and restores the media type in its `finally`, on success and on failure. Only detection uses a scratch copy.

- [ ] **Step 4: Run tests to verify they pass**

Run: same as Step 2. Expected: `exit=0`.

- [ ] **Step 5: Commit**

Use `/commit` with `AutoLanguageOCRParser.java` and its test. Message: `feat(ocr): pick the tesseract model per image from its detected script`.

---

### Task 5: Extractor wiring, routing tests and CI (about 2 hours, plus suite runtime)

**Files:**
- Modify: `extract-lib/src/main/java/org/icij/extract/extractor/Extractor.java` (`@Option` `:107-109`, fields near `:206`, `configure` `:274-279`, `setOcrConfig` `:370-381`, `setOcrLanguage` `:520-522`, `disableOcr` `:600-610`, getters near `:412`)
- Modify: `.circleci/config.yml:40`
- Test: `extract-lib/src/test/java/org/icij/extract/extractor/AutoLanguageOCRTest.java` (create)
- Test: `extract-lib/src/test/java/org/icij/extract/extractor/ExtractorAutoLanguageTest.java` (create)

**Interfaces:**
- Consumes: `AutoLanguageOCRParser` and its constants (Task 4), `OCRConfigAdapter.installedModels()` (Task 3), fixtures (Task 2).
- Produces: option `ocrRetryConfidence`; `public int Extractor.getOcrRetryConfidence()`; package-private `Parser Extractor.withAutoLanguage(OCRConfigAdapter<?> ocrConfig, Parser ocrParser)`.

- [ ] **Step 1: Write the routing and wiring tests**

`AutoLanguageOCRTest.java`:

```java
package org.icij.extract.extractor;

import org.apache.tika.metadata.Metadata;
import org.icij.extract.document.TikaDocument;
import org.icij.spewer.Spewer;
import org.icij.task.Options;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Reader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.fest.assertions.Assertions.assertThat;
import static org.icij.extract.ocr.AutoLanguageOCRParser.OCR_MODEL;
import static org.icij.extract.ocr.AutoLanguageOCRParser.OCR_SCRIPT;
import static org.icij.extract.ocr.ParserWithConfidence.OCR_CONFIDENCE;

@RunWith(Parameterized.class)
public class AutoLanguageOCRTest {
    @Parameterized.Parameters(name = "{0} {1}")
    public static List<Object[]> fixtures() {
        List<Object[]> rows = new ArrayList<>();
        for (String backend : List.of("TESSERACT", "TESS4J")) {
            rows.add(new Object[] {backend, "latin", "Latin", "script/Latin", List.of("city council published", "public library")});
            rows.add(new Object[] {backend, "cyrillic", "Cyrillic", "script/Cyrillic+script/Latin", List.of("Городской совет", "публичной библиотеке")});
            rows.add(new Object[] {backend, "arabic", "Arabic", "script/Arabic+script/Latin", List.of("مجلس المدينة", "المكتبة العامة")});
            rows.add(new Object[] {backend, "hans", "Han", "script/HanS+script/Latin", List.of("预算报告", "公共图书馆")});
            rows.add(new Object[] {backend, "japanese", "Japanese", "script/Japanese+script/Latin", List.of("予算", "公共図書館")});
            rows.add(new Object[] {backend, "hangul", "Korean", "script/Hangul+script/Latin", List.of("예산", "월요일")});
        }
        return rows;
    }

    @Parameterized.Parameter(0) public String backend;
    @Parameterized.Parameter(1) public String fixture;
    @Parameterized.Parameter(2) public String script;
    @Parameterized.Parameter(3) public String model;
    @Parameterized.Parameter(4) public List<String> words;

    @Test
    public void test_reads_the_image_in_its_script() throws Exception {
        // Given
        Extractor extractor = new Extractor(Options.from(Map.of("ocrType", backend)));
        // When
        TikaDocument document = extractor.extract(Paths.get(getClass().getResource("/documents/ocr/auto/" + fixture + ".png").getPath()));
        String text;
        try (Reader reader = document.getReader()) {
            text = Spewer.toString(reader);
        }
        // Then
        Metadata metadata = document.getMetadata();
        assertThat(metadata.get(OCR_SCRIPT)).isEqualTo(script);
        assertThat(metadata.get(OCR_MODEL)).isEqualTo(model);
        assertThat(Double.parseDouble(metadata.get(OCR_CONFIDENCE))).isGreaterThanOrEqualTo(0.6);
        for (String word : words) {
            assertThat(squash(text)).contains(squash(word));
        }
    }

    static String squash(String text) {
        return text.replaceAll("\\s+", "");
    }
}
```

Words are compared without whitespace because tesseract splits CJK text into spaced runs (`预算 报告`, measured while planning).

`ExtractorAutoLanguageTest.java`:

```java
package org.icij.extract.extractor;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.tika.metadata.Metadata;
import org.icij.extract.document.TikaDocument;
import org.icij.spewer.Spewer;
import org.icij.task.Options;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import static org.fest.assertions.Assertions.assertThat;
import static org.icij.extract.extractor.AutoLanguageOCRTest.squash;
import static org.icij.extract.ocr.AutoLanguageOCRParser.OCR_MODEL;
import static org.icij.extract.ocr.AutoLanguageOCRParser.OCR_SCRIPT;

public class ExtractorAutoLanguageTest {
    private static final String ALL = "script/Latin+script/HanS+script/Cyrillic+script/Arabic+script/Japanese+script/Hangul";

    @Test
    public void test_mixed_image_keeps_every_script() throws Exception {
        for (String backend : List.of("TESSERACT", "TESS4J")) {
            // Given
            Extractor extractor = new Extractor(Options.from(Map.of("ocrType", backend)));
            // When
            String text = textOf(extractor.extract(path("/documents/ocr/auto/mixed.png")));
            // Then
            assertThat(squash(text)).as(backend).contains("预算报告");
            assertThat(squash(text)).as(backend).contains("Городскойсовет");
            assertThat(squash(text)).as(backend).contains("citycouncil");
        }
    }

    @Test
    public void test_retry_confidence_zero_keeps_the_first_pass() throws Exception {
        // Given
        Extractor extractor = new Extractor(Options.from(Map.of("ocrRetryConfidence", "0")));
        // When
        TikaDocument document = extractor.extract(path("/documents/ocr/auto/mixed.png"));
        textOf(document);
        // Then
        assertThat(document.getMetadata().get(OCR_MODEL)).isNotNull();
        assertThat(document.getMetadata().get(OCR_MODEL)).isNotEqualTo(ALL);
    }

    @Test
    public void test_too_little_text_for_detection_reads_as_latin() throws Exception {
        // Given
        Extractor extractor = new Extractor();
        // When
        TikaDocument document = extractor.extract(path("/documents/ocr/simple.tiff"));
        String text = textOf(document);
        // Then
        assertThat(text).contains("HEAVY");
        assertThat(text).contains("METAL");
        assertThat(document.getMetadata().get(OCR_SCRIPT)).isNull();
        assertThat(document.getMetadata().get(OCR_MODEL)).isEqualTo("script/Latin");
    }

    @Test
    public void test_every_page_of_a_multipage_tiff_is_read() throws Exception {
        // Given
        Extractor extractor = new Extractor();
        // When
        TikaDocument document = extractor.extract(path("/documents/ocr/test_tiff_multipage.tif"));
        String text = textOf(document);
        // Then
        assertThat(document.getMetadata().get(OCR_MODEL)).isNotNull();
        assertThat(text.replaceAll("[^A-Za-z0-9]", " ")).contains("Page 2");
    }

    @Test
    public void test_explicit_language_skips_routing() throws Exception {
        // Given
        Extractor extractor = new Extractor(Options.from(Map.of("ocrLanguage", "eng")));
        // When
        TikaDocument document = extractor.extract(path("/documents/ocr/simple.tiff"));
        String text = textOf(document);
        // Then
        assertThat(text.trim()).isEqualTo("HEAVY\nMETAL");
        assertThat(document.getMetadata().get(OCR_MODEL)).isNull();
    }

    @Test
    public void test_setting_a_language_after_construction_skips_routing() throws Exception {
        // Given
        Extractor extractor = new Extractor();
        extractor.setOcrLanguage("eng");
        // When
        TikaDocument document = extractor.extract(path("/documents/ocr/simple.tiff"));
        String text = textOf(document);
        // Then
        assertThat(text.trim()).isEqualTo("HEAVY\nMETAL");
        assertThat(document.getMetadata().get(OCR_MODEL)).isNull();
    }

    private static java.nio.file.Path path(String resource) {
        return Paths.get(ExtractorAutoLanguageTest.class.getResource(resource).getPath());
    }

    private static String textOf(TikaDocument document) throws IOException {
        try (Reader reader = document.getReader()) {
            return Spewer.toString(reader);
        }
    }
}
```

- [ ] **Step 2: Run the tests to see them fail**

Run (background): `cd extract-lib && LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH mvn -q -o test -Dtest='AutoLanguageOCRTest,ExtractorAutoLanguageTest' > /tmp/t5a.log 2>&1; echo "exit=$?"`
Expected, show the reviewer:
- `AutoLanguageOCRTest`: all 12 fail, `ocr:script` is null.
- `test_mixed_image_keeps_every_script`, `test_retry_confidence_zero_keeps_the_first_pass`, `test_too_little_text_for_detection_reads_as_latin`, `test_every_page_of_a_multipage_tiff_is_read`: fail, `ocr:model` null or Chinese missing.
- `test_explicit_language_skips_routing`, `test_setting_a_language_after_construction_skips_routing`: pass. They pin today's behavior; Step 7 makes them fail on purpose.

- [ ] **Step 3: Add the option and startup-check tests**

Append to `ExtractorAutoLanguageTest` (import `org.apache.tika.parser.EmptyParser`, `org.apache.tika.parser.Parser`, `org.icij.extract.ocr.TesseractOCRConfigAdapter`, `java.util.Set`):

```java
    @Test
    public void test_retry_confidence_is_clamped_to_0_100() {
        assertThat(new Extractor(Options.from(Map.of("ocrRetryConfidence", "150"))).getOcrRetryConfidence()).isEqualTo(100);
        assertThat(new Extractor(Options.from(Map.of("ocrRetryConfidence", "-5"))).getOcrRetryConfidence()).isEqualTo(0);
        assertThat(new Extractor().getOcrRetryConfidence()).isEqualTo(60);
    }

    @Test
    public void test_missing_models_keep_plain_ocr_and_warn_once() {
        // Given
        TesseractOCRConfigAdapter partial = new TesseractOCRConfigAdapter() {
            @Override
            public Set<String> installedModels() {
                return Set.of("osd", "script/Latin");
            }
        };
        Extractor extractor = new Extractor();
        Logger log = (Logger) LoggerFactory.getLogger(Extractor.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        log.addAppender(appender);
        // When
        Parser installed;
        try {
            installed = extractor.withAutoLanguage(partial, EmptyParser.INSTANCE);
        } finally {
            log.detachAppender(appender);
        }
        // Then
        assertThat(installed).isSameAs(EmptyParser.INSTANCE);
        List<ILoggingEvent> warnings = appender.list.stream().filter(e -> e.getLevel() == Level.WARN).toList();
        assertThat(warnings).hasSize(1);
        assertThat(warnings.get(0).getFormattedMessage()).contains("script/HanS");
    }
```

- [ ] **Step 4: Run to see them fail**

Run: same as Step 2. Expected: compilation errors `cannot find symbol: method getOcrRetryConfidence()` and `method withAutoLanguage(...)`.

- [ ] **Step 5: Implement the wiring**

Imports in `Extractor.java`: `org.icij.extract.ocr.AutoLanguageOCRParser`, `java.util.Set`.

Replace the `ocrLanguage` `@Option` (`:107-109`) with:

```java
@Option(name = "ocrLanguage", description = "Set the languages used by Tesseract. Multiple languages may be " +
        "specified, separated by plus characters. Tesseract uses 3-character ISO 639-2 language codes. " +
        "When absent, the language is chosen per image from its detected script if the osd and script " +
        "models are installed, otherwise English is used.", parameter = "language")
@Option(name = "ocrRetryConfidence", description = "When the OCR language is chosen per image, read an image " +
        "again with every supported script if the first read's mean word confidence is below this value, " +
        "from 0 to 100. Defaults to 60.", parameter = "confidence")
```

Fields, next to `private boolean ocrFanout = true;`:

```java
    private boolean autoOcrLanguage = true;
    private int ocrRetryConfidence = 60;
```

In `configure`, replace:

```java
        options.get("ocrType", String.valueOf(OCRConfigRegistry.TESSERACT))
            .parse()
            .asEnum(OCRConfigRegistry::parse)
            .map(OCRConfigRegistry::buildAdapter)
            .ifPresent(this::setOcrConfig);
        options.get("ocrLanguage", "eng").value().ifPresent(this::setOcrLanguage);
```

with:

```java
        final Optional<String> ocrLanguage = options.valueIfPresent("ocrLanguage");
        autoOcrLanguage = ocrLanguage.isEmpty() && !options.get("ocr", "true").parse().isOff();
        options.get("ocrRetryConfidence", "60").parse().asInteger().ifPresent(n -> {
            if (n < 0 || n > 100) {
                logger.warn("ocrRetryConfidence {} is outside 0 to 100; clamping.", n);
            }
            this.ocrRetryConfidence = Math.max(0, Math.min(100, n));
        });
        options.get("ocrType", String.valueOf(OCRConfigRegistry.TESSERACT))
            .parse()
            .asEnum(OCRConfigRegistry::parse)
            .map(OCRConfigRegistry::buildAdapter)
            .ifPresent(this::setOcrConfig);
        ocrLanguage.ifPresent(this::setOcrLanguage);
```

(Dropping the `"eng"` default is safe: `TesseractOCRConfig` already defaults to `eng`, `TesseractOCRConfig.java:75`.)

In `setOcrConfig`, replace `Parser ocrParser = ocrConfig.buildParser();` with:

```java
        Parser ocrParser = withAutoLanguage(ocrConfig, ocrConfig.buildParser());
```

Add after `setOcrConfig`:

```java
    Parser withAutoLanguage(final OCRConfigAdapter<?> ocrConfig, final Parser ocrParser) {
        if (!autoOcrLanguage) {
            return ocrParser;
        }
        final Set<String> missing = AutoLanguageOCRParser.missingModels(ocrConfig.installedModels());
        if (!missing.isEmpty()) {
            logger.warn("OCR language detection disabled, missing tesseract models {}; OCR uses \"{}\".",
                    missing, ocrConfig.getConfig().getLanguage());
            autoOcrLanguage = false;
            return ocrParser;
        }
        return new AutoLanguageOCRParser(ocrParser, ocrConfig instanceof TesseractOCRConfigAdapter,
                ocrRetryConfidence, () -> autoOcrLanguage);
    }
```

`setOcrLanguage`:

```java
    public void setOcrLanguage(final String ocrLanguage) {
        autoOcrLanguage = false;
        ocrConfig.setLanguages(ocrLanguage.split("\\+"));
    }
```

`disableOcr`, inside the `if`, after `excludeParser(OCRParserAdapter.class);`:

```java
            excludeParser(AutoLanguageOCRParser.class);
```

Getter, next to `getMaxEmbedSizeBytes()`:

```java
    public int getOcrRetryConfidence() { return ocrRetryConfidence; }
```

- [ ] **Step 6: Run the new tests**

Run: same as Step 2. Expected: `exit=0`.

- [ ] **Step 7: Watch the two guard tests fail on purpose**

1. Delete `autoOcrLanguage = false;` from `setOcrLanguage`, run `-Dtest=ExtractorAutoLanguageTest#test_setting_a_language_after_construction_skips_routing`. Expected: FAIL, `ocr:model` is `script/Latin`. Restore the line.
2. Delete `excludeParser(AutoLanguageOCRParser.class);` from `disableOcr`, run `-Dtest=ExtractorTest#testDisableOcr`. Expected: FAIL, text not empty. Restore the line.
3. In `configure`, temporarily set `autoOcrLanguage = true;` (ignoring `ocrLanguage`), run `-Dtest=ExtractorAutoLanguageTest#test_explicit_language_skips_routing`. Expected: FAIL. Restore.

Show the reviewer the three failure lines.

- [ ] **Step 8: Run the existing OCR tests as the English regression check (background)**

Run: `cd extract-lib && LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH mvn -q -o test -Dtest='ExtractorTest,OCRParserAdapterTest,ImageIOTranscodingOCRParserTest,Tess4JOCRParserTest,*Ocr*Test,*OCR*Test' > /tmp/t5b.log 2>&1; echo "exit=$?"`
Expected: `exit=0`. A failure whose only difference is whitespace (for example `"HEAVY\n\nMETAL"` against `"HEAVY\nMETAL"` in `ExtractorTest.testOcr`) gets its expected text updated and listed for the PR. Any other failure is a bug: stop and debug.

- [ ] **Step 9: Install the models in CI**

In `.circleci/config.yml`, replace line 40:

```yaml
            sudo apt-get install -y libtesseract5 tesseract-ocr tesseract-ocr-eng libpng-dev libjpeg-dev libtiff5-dev libwebp-dev libopenjp2-7-dev libgif-dev
```

with:

```yaml
            sudo apt-get install -y libtesseract5 tesseract-ocr tesseract-ocr-eng tesseract-ocr-osd tesseract-ocr-script-latn tesseract-ocr-script-hans tesseract-ocr-script-cyrl tesseract-ocr-script-arab tesseract-ocr-script-jpan tesseract-ocr-script-hang libpng-dev libjpeg-dev libtiff5-dev libwebp-dev libopenjp2-7-dev libgif-dev
            TESSDATA=$(tesseract --list-langs 2>&1 | sed -n 's/.*"\(.*\)".*/\1/p')
            sudo mkdir -p "$TESSDATA/script"
            for model in Latin HanS Cyrillic Arabic Japanese Hangul; do sudo ln -sf "$(find /usr/share/tesseract-ocr -name "$model.traineddata" -not -path '*/script/*' | head -1)" "$TESSDATA/script/$model.traineddata"; done
            [ -e "${TESSDATA}osd.traineddata" ] || sudo ln -s "$(find /usr/share/tesseract-ocr -name osd.traineddata | head -1)" "${TESSDATA}osd.traineddata"
            tesseract --list-langs 2>&1 | grep -E '^(osd|script/)'
```

The `find` handles packages that install into a different tessdata directory than the notesalexp tesseract reads. The last line prints the 7 models in the CI log. Check locally that the block is valid shell: run lines 2 to 6 in `bash` in the container. Expected: the same 7 lines as "Before Task 1", Step 2.

- [ ] **Step 10: Full suite (background)**

Run: `LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH mvn -o test > /tmp/full.log 2>&1; echo "exit=$?"`. Poll `/tmp/full.log` and report each check. Expected: `BUILD SUCCESS`. Separate pre-existing failures (rerun on `master` to confirm) from ones this branch introduced.

- [ ] **Step 11: Commit**

Use `/commit` with `Extractor.java`, the two new tests, any whitespace-only expectation updates, and `.circleci/config.yml`. Message: `feat(extractor): choose the OCR language per image when none is given`. Then ask the maintainer to check CI before pushing.

---

## Suggested follow-ups (not in this plan)

- `ocrCache` wraps nothing for standard images: `Extractor.java:299` targets `TesseractOCRParser.class`, but the tree holds `OCRParserAdapter` after `setOcrConfig` (`Extractor.java:373`). Only transcoded images are cached today.
- `OSDRunner` and `TextOCRRunner` use `Optional.orElse(tesseract.getOSD(image))`, which runs OSD twice when a processed image file exists (TIFF, preprocessing). `orElseGet` fixes it.
- Datashare: bump extract, install `tesseract-ocr-osd` and the script models under `tessdata/script/` in Docker, snap and Windows, feed `ocr:*` into Lingua.
- Cache the installed-model probe per JVM if Datashare builds many `Extractor`s (one `tesseract --list-langs` per construction).
