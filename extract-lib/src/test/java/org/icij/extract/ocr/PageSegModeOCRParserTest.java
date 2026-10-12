package org.icij.extract.ocr;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.ocr.TesseractOCRConfig;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.XHTMLContentHandler;
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
import static org.icij.extract.ocr.PageSegModeOCRParser.OCR_PAGE_SEG_MODE;
import static org.junit.Assert.assertThrows;

/**
 * An image is read with each page segmentation mode in turn until one finds body text, and the caller's
 * tesseract config is restored afterwards.
 */
public class PageSegModeOCRParserTest {
    private static final List<String> AUTO = List.of("1", "6", "11");

    @Test
    public void test_a_read_with_text_in_mode_1_runs_once() throws Exception {
        // Given
        StubOcr stub = new StubOcr();
        stub.text.put("1", "layout text");
        Metadata metadata = new Metadata();
        // When
        String text = parse(new PageSegModeOCRParser(stub, AUTO), metadata, new ParseContext());
        // Then
        assertThat(stub.calls).isEqualTo(List.of("1"));
        assertThat(text).isEqualTo("layout text");
        assertThat(metadata.get(OCR_PAGE_SEG_MODE)).isEqualTo("1");
    }

    @Test
    public void test_a_blank_read_in_mode_1_falls_back_to_mode_6() throws Exception {
        // Given
        StubOcr stub = new StubOcr();
        stub.text.put("6", "block text");
        Metadata metadata = new Metadata();
        // When
        String text = parse(new PageSegModeOCRParser(stub, AUTO), metadata, new ParseContext());
        // Then
        assertThat(stub.calls).isEqualTo(List.of("1", "6"));
        assertThat(text).isEqualTo("block text");
        assertThat(metadata.get(OCR_PAGE_SEG_MODE)).isEqualTo("6");
    }

    @Test
    public void test_blank_reads_in_modes_1_and_6_fall_back_to_mode_11() throws Exception {
        // Given
        StubOcr stub = new StubOcr();
        stub.text.put("11", "sparse text");
        Metadata metadata = new Metadata();
        // When
        String text = parse(new PageSegModeOCRParser(stub, AUTO), metadata, new ParseContext());
        // Then
        assertThat(stub.calls).isEqualTo(List.of("1", "6", "11"));
        assertThat(text).isEqualTo("sparse text");
        assertThat(metadata.get(OCR_PAGE_SEG_MODE)).isEqualTo("11");
    }

    @Test
    public void test_an_image_blank_in_every_mode_keeps_the_last_read() throws Exception {
        // Given
        StubOcr stub = new StubOcr();
        Metadata metadata = new Metadata();
        // When
        String text = parse(new PageSegModeOCRParser(stub, AUTO), metadata, new ParseContext());
        // Then
        assertThat(stub.calls).isEqualTo(List.of("1", "6", "11"));
        assertThat(text).isEmpty();
        assertThat(metadata.get(OCR_PAGE_SEG_MODE)).isEqualTo("11");
    }

    @Test
    public void test_a_forced_mode_reads_once_even_when_blank() throws Exception {
        // Given
        StubOcr stub = new StubOcr();
        Metadata metadata = new Metadata();
        // When
        parse(new PageSegModeOCRParser(stub, List.of("4")), metadata, new ParseContext());
        // Then
        assertThat(stub.calls).isEqualTo(List.of("4"));
        assertThat(metadata.get(OCR_PAGE_SEG_MODE)).isEqualTo("4");
    }

    @Test
    public void test_whitespace_counts_as_a_blank_read() throws Exception {
        // Given
        StubOcr stub = new StubOcr();
        stub.text.put("1", " \n\t ");
        stub.text.put("6", "block text");
        // When
        parse(new PageSegModeOCRParser(stub, AUTO), new Metadata(), new ParseContext());
        // Then
        assertThat(stub.calls).isEqualTo(List.of("1", "6"));
    }

    @Test
    public void test_a_title_does_not_count_as_text() throws Exception {
        // Given
        StubOcr stub = new StubOcr();
        stub.text.put("6", "block text");
        Metadata metadata = new Metadata();
        metadata.set(TikaCoreProperties.TITLE, "screenshot");
        // When
        parse(new PageSegModeOCRParser(stub, AUTO), metadata, new ParseContext());
        // Then
        assertThat(stub.calls).isEqualTo(List.of("1", "6"));
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
        parse(new PageSegModeOCRParser(stub, AUTO), metadata, context);
        // Then
        assertThat(stub.calls).isEqualTo(List.of("0"));
        assertThat(metadata.get(OCR_PAGE_SEG_MODE)).isNull();
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
        parse(new PageSegModeOCRParser(stub, AUTO), metadata, context);
        // Then
        assertThat(stub.calls).hasSize(1);
        assertThat(metadata.get(OCR_PAGE_SEG_MODE)).isNull();
    }

    @Test
    public void test_caller_settings_reach_every_read() throws Exception {
        // Given
        StubOcr stub = new StubOcr();
        TesseractOCRConfig caller = new TesseractOCRConfig();
        caller.setTimeoutSeconds(42);
        ParseContext context = new ParseContext();
        context.set(TesseractOCRConfig.class, caller);
        // When
        parse(new PageSegModeOCRParser(stub, AUTO), new Metadata(), context);
        // Then
        assertThat(stub.timeouts).isEqualTo(List.of(42, 42, 42));
        assertThat(context.get(TesseractOCRConfig.class)).isSameAs(caller);
    }

    @Test
    public void test_caller_config_is_restored_after_a_failed_read() {
        // Given
        StubOcr stub = new StubOcr();
        stub.failingMode = "6";
        TesseractOCRConfig caller = new TesseractOCRConfig();
        ParseContext context = new ParseContext();
        context.set(TesseractOCRConfig.class, caller);
        // When
        TikaException error = assertThrows(TikaException.class,
                () -> parse(new PageSegModeOCRParser(stub, AUTO), new Metadata(), context));
        // Then
        assertThat(error.getMessage()).isEqualTo("OCR timeout");
        assertThat(context.get(TesseractOCRConfig.class)).isSameAs(caller);
    }

    @Test
    public void test_parses_auto_in_any_case_and_forced_modes() {
        assertThat(PageSegModes.parse("AUTO")).isEqualTo(AUTO);
        assertThat(PageSegModes.parse("auto")).isEqualTo(AUTO);
        assertThat(PageSegModes.parse("6")).isEqualTo(List.of("6"));
        assertThat(PageSegModes.parse(" 13 ")).isEqualTo(List.of("13"));
    }

    @Test
    public void test_rejects_modes_without_text_and_unknown_values() {
        for (String value : List.of("0", "2", "14", "abc", "")) {
            InvalidPageSegMode error = assertThrows(InvalidPageSegMode.class,
                    () -> PageSegModes.parse(value));
            assertThat(error.value).as(value).isEqualTo(value);
            assertThat(error.getMessage()).as(value).contains("AUTO");
        }
    }

    private static String parse(Parser parser, Metadata metadata, ParseContext context) throws Exception {
        BodyContentHandler handler = new BodyContentHandler(-1);
        parser.parse(new ByteArrayInputStream(new byte[] {1, 2, 3}), handler, metadata, context);
        return handler.toString().strip();
    }

    private static class StubOcr implements Parser {
        final List<String> calls = new ArrayList<>();
        final List<Integer> timeouts = new ArrayList<>();
        final Map<String, String> text = new HashMap<>();
        String failingMode = null;

        @Override
        public Set<MediaType> getSupportedTypes(ParseContext context) {
            return Set.of(MediaType.image("ocr-png"));
        }

        @Override
        public void parse(InputStream stream, ContentHandler handler, Metadata metadata, ParseContext context)
                throws SAXException, TikaException {
            TesseractOCRConfig config = context.get(TesseractOCRConfig.class, new TesseractOCRConfig());
            calls.add(config.getPageSegMode());
            timeouts.add(config.getTimeoutSeconds());
            if (config.getPageSegMode().equals(failingMode)) {
                throw new TikaException("OCR timeout");
            }
            XHTMLContentHandler xhtml = new XHTMLContentHandler(handler, metadata);
            xhtml.startDocument();
            xhtml.element("div", text.getOrDefault(config.getPageSegMode(), ""));
            xhtml.endDocument();
        }
    }
}
