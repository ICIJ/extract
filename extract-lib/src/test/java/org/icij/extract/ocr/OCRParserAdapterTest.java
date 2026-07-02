package org.icij.extract.ocr;

import static org.apache.tika.metadata.TikaCoreProperties.CONTENT_TYPE_PARSER_OVERRIDE;
import static org.fest.assertions.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Set;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.junit.Test;
import org.xml.sax.ContentHandler;

public class OCRParserAdapterTest {

    // Stand-in OCR parser: does nothing, mirroring the state after Tika's AbstractImageParser has
    // routed the image (CONTENT_TYPE_PARSER_OVERRIDE already set to the "image/ocr-*" type).
    private static class NoopParser implements Parser {
        @Override
        public Set<MediaType> getSupportedTypes(ParseContext context) {
            return Set.of(MediaType.image("ocr-jpeg"));
        }

        @Override
        public void parse(InputStream stream, ContentHandler handler, Metadata metadata, ParseContext context) {
            // no-op: the routing type is set by the caller (AbstractImageParser) before we run
        }
    }

    @Test
    public void test_restores_real_media_type_from_ocr_routing_type() throws Exception {
        OCRParserAdapter<NoopParser> adapter = new OCRParserAdapter<>(new NoopParser());
        Metadata metadata = new Metadata();
        metadata.set(CONTENT_TYPE_PARSER_OVERRIDE, "image/ocr-jpeg");
        metadata.set(Metadata.CONTENT_TYPE, "image/ocr-jpeg");

        adapter.parse(new ByteArrayInputStream(new byte[0]), new BodyContentHandler(), metadata, new ParseContext());

        assertThat(metadata.get(Metadata.CONTENT_TYPE)).isEqualTo("image/jpeg");
        assertThat(metadata.get(CONTENT_TYPE_PARSER_OVERRIDE)).isEqualTo("image/jpeg");
        assertThat(metadata.get(OCRParser.OCR_PARSER)).isEqualTo(NoopParser.class.getName());
    }

    @Test
    public void test_leaves_non_ocr_content_type_untouched() throws Exception {
        OCRParserAdapter<NoopParser> adapter = new OCRParserAdapter<>(new NoopParser());
        Metadata metadata = new Metadata();
        metadata.set(Metadata.CONTENT_TYPE, "image/jpeg");

        adapter.parse(new ByteArrayInputStream(new byte[0]), new BodyContentHandler(), metadata, new ParseContext());

        assertThat(metadata.get(Metadata.CONTENT_TYPE)).isEqualTo("image/jpeg");
    }

    @Test
    public void test_strip_ocr_prefix() {
        assertThat(OCRParserAdapter.stripOcrPrefix("image/ocr-jpeg")).isEqualTo("image/jpeg");
        assertThat(OCRParserAdapter.stripOcrPrefix("image/ocr-x-portable-pixmap")).isEqualTo("image/x-portable-pixmap");
        assertThat(OCRParserAdapter.stripOcrPrefix("image/jpeg")).isNull();
        assertThat(OCRParserAdapter.stripOcrPrefix("application/pdf")).isNull();
        assertThat(OCRParserAdapter.stripOcrPrefix(null)).isNull();
    }
}
