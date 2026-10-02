package org.icij.extract.ocr;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.ocr.TesseractOCRConfig;
import org.apache.tika.parser.ocr.TesseractOCRParser;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.ToXMLContentHandler;
import org.apache.tika.sax.XHTMLContentHandler;
import org.fest.assertions.Delta;
import org.junit.Test;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
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
    public void test_a_failed_retry_keeps_the_first_pass() throws Exception {
        // Given
        StubOcr stub = new StubOcr();
        stub.confidence.put("script/HanS+script/Latin", 0.3);
        stub.failingModel = ALL;
        Metadata metadata = new Metadata();
        // When
        String text = parse(router(stub, 60), metadata, new ParseContext());
        // Then
        assertThat(stub.calls).isEqualTo(List.of("osd", "script/HanS+script/Latin", ALL));
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
        assertThat(stub.calls).isEqualTo(List.of("osd", "script/Latin"));
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
    public void test_text_ends_with_a_newline_like_tesseract_text_output() throws Exception {
        // Given
        ToXMLContentHandler handler = new ToXMLContentHandler();
        // When
        router(new StubOcr(), 60).parse(image(), handler, new Metadata(), new ParseContext());
        // Then
        assertThat(handler.toString()).contains("<div class=\"ocr\">read with script/HanS+script/Latin\n</div>");
    }

    @Test
    public void test_routing_metadata_reaches_the_html_head() throws Exception {
        // Given
        ToXMLContentHandler handler = new ToXMLContentHandler();
        // When
        router(new StubOcr(), 60).parse(image(), handler, new Metadata(), new ParseContext());
        // Then
        assertThat(handler.toString()).contains("<meta name=\"ocr:model\" content=\"script/HanS+script/Latin\"");
    }

    @Test
    public void test_skipped_ocr_passes_straight_through() throws Exception {
        // Given
        StubOcr stub = new StubOcr();
        TesseractOCRConfig caller = new TesseractOCRConfig();
        caller.setSkipOcr(true);
        ParseContext context = new ParseContext();
        context.set(TesseractOCRConfig.class, caller);
        Metadata metadata = new Metadata();
        // When
        parse(router(stub, 60), metadata, context);
        // Then
        assertThat(stub.calls).isEqualTo(List.of("eng"));
        assertThat(metadata.get(OCR_MODEL)).isNull();
    }

    @Test
    public void test_an_image_above_the_ocr_size_limit_passes_straight_through() throws Exception {
        // Given
        StubOcr stub = new StubOcr();
        TesseractOCRConfig caller = new TesseractOCRConfig();
        caller.setMaxFileSizeToOcr(2);
        ParseContext context = new ParseContext();
        context.set(TesseractOCRConfig.class, caller);
        Metadata metadata = new Metadata();
        // When
        parse(router(stub, 60), metadata, context);
        // Then
        assertThat(stub.calls).isEqualTo(List.of("eng"));
        assertThat(metadata.get(OCR_MODEL)).isNull();
    }

    @Test
    public void test_a_caller_asking_for_osd_passes_straight_through() throws Exception {
        // Given
        StubOcr stub = new StubOcr();
        TesseractOCRConfig caller = new TesseractOCRConfig();
        caller.setPageSegMode("0");
        ParseContext context = new ParseContext();
        context.set(TesseractOCRConfig.class, caller);
        Metadata metadata = new Metadata();
        // When
        parse(router(stub, 60), metadata, context);
        // Then
        assertThat(stub.calls).isEqualTo(List.of("osd"));
        assertThat(metadata.get(TesseractOCRParser.PSM0_SCRIPT)).isEqualTo("Han");
        assertThat(metadata.get(OCR_MODEL)).isNull();
    }

    @Test
    public void test_content_type_is_restored_when_the_image_cannot_be_read() {
        // Given
        Metadata metadata = new Metadata();
        metadata.set(Metadata.CONTENT_TYPE, "image/ocr-png");
        InputStream broken = new InputStream() {
            @Override
            public int read() throws IOException {
                throw new IOException("disk full");
            }
        };
        // When
        assertThrows(IOException.class, () -> router(new StubOcr(), 60)
                .parse(broken, new BodyContentHandler(-1), metadata, new ParseContext()));
        // Then
        assertThat(metadata.get(Metadata.CONTENT_TYPE)).isEqualTo("image/png");
    }

    @Test
    public void test_retry_metadata_does_not_reach_the_document() throws Exception {
        // Given
        StubOcr stub = new StubOcr();
        stub.confidence.put("script/HanS+script/Latin", 0.3);
        stub.confidence.put(ALL, 0.2);
        Metadata metadata = new Metadata();
        // When
        parse(router(stub, 60), metadata, new ParseContext());
        // Then
        assertThat(stub.calls).isEqualTo(List.of("osd", "script/HanS+script/Latin", ALL));
        assertThat(metadata.getValues("stub:read")).isEqualTo(new String[] {"script/HanS+script/Latin"});
    }

    @Test
    public void test_a_runtime_failure_in_detection_reads_as_latin() throws Exception {
        // Given
        StubOcr stub = new StubOcr();
        stub.osdFailure = new NumberFormatException("For input string: \"nan\"");
        Metadata metadata = new Metadata();
        // When
        parse(router(stub, 60), metadata, new ParseContext());
        // Then
        assertThat(stub.calls).isEqualTo(List.of("osd", "script/Latin"));
        assertThat(metadata.get(OCR_MODEL)).isEqualTo("script/Latin");
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
        parser.parse(image(), handler, metadata, context);
        return handler.toString().strip();
    }

    private static InputStream image() {
        return new ByteArrayInputStream(new byte[] {1, 2, 3});
    }

    private static class StubOcr implements Parser {
        final List<String> calls = new ArrayList<>();
        final List<Integer> timeouts = new ArrayList<>();
        final Map<String, Double> confidence = new HashMap<>();
        String script = "Han";
        String text = null;
        boolean failOcr = false;
        String failingModel = null;
        RuntimeException osdFailure = null;

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
                if (osdFailure != null) {
                    throw osdFailure;
                }
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
            if (config.getLanguage().equals(failingModel)) {
                throw new TikaException("OCR timeout");
            }
            metadata.set(OCR_CONFIDENCE, confidence.getOrDefault(config.getLanguage(), 0.9));
            metadata.add("stub:read", config.getLanguage());
            XHTMLContentHandler xhtml = new XHTMLContentHandler(handler, metadata);
            xhtml.startDocument();
            xhtml.element("div", text == null ? "read with " + config.getLanguage() : text);
            xhtml.endDocument();
        }
    }
}
