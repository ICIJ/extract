package org.icij.extract.ocr;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.junit.Test;
import org.xml.sax.ContentHandler;
import org.xml.sax.helpers.DefaultHandler;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

import static org.apache.tika.metadata.TikaCoreProperties.CONTENT_TYPE_PARSER_OVERRIDE;
import static org.fest.assertions.Assertions.assertThat;

public class ImageIOTranscodingOCRParserTest {

    private static Parser noopOcrParser() {
        return new Parser() {
            @Override
            public Set<MediaType> getSupportedTypes(ParseContext context) {
                return Set.of(MediaType.parse("image/ocr-png"), MediaType.parse("image/jp2"));
            }
            @Override
            public void parse(java.io.InputStream s, org.xml.sax.ContentHandler h,
                              org.apache.tika.metadata.Metadata m, ParseContext c) {}
        };
    }

    // Captures the stream and metadata the fallback hands to the OCR delegate.
    private static class CapturingOcrParser implements Parser {
        Metadata seenMetadata;
        byte[] seenBytes;
        @Override
        public Set<MediaType> getSupportedTypes(ParseContext context) {
            return Set.of();
        }
        @Override
        public void parse(InputStream s, ContentHandler h, Metadata m, ParseContext c) throws IOException {
            this.seenMetadata = m;
            this.seenBytes = s.readAllBytes();
        }
    }

    private static byte[] pngBytes() throws IOException {
        BufferedImage image = new BufferedImage(8, 8, BufferedImage.TYPE_INT_RGB);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ImageIO.write(image, "png", out);
        return out.toByteArray();
    }

    @Test
    public void testClaimsJbig2() {
        Set<MediaType> types = new ImageIOTranscodingOCRParser(noopOcrParser())
                .getSupportedTypes(new ParseContext());
        assertThat(types).contains(MediaType.parse("image/x-jbig2"));
        assertThat(types).contains(MediaType.parse("image/x-jb2"));
    }

    @Test
    public void testDoesNotClaimStandardRasterTypes() {
        Set<MediaType> types = new ImageIOTranscodingOCRParser(noopOcrParser())
                .getSupportedTypes(new ParseContext());
        assertThat(types).excludes(
                MediaType.parse("image/png"),
                MediaType.parse("image/jpeg"),
                MediaType.parse("image/tiff"),
                MediaType.parse("image/bmp"),
                MediaType.parse("image/gif"),
                MediaType.parse("image/jp2"));
    }

    @Test
    public void testSetsOcrFormatOverrideAndDelegatesDecodablePng() throws Exception {
        CapturingOcrParser ocr = new CapturingOcrParser();
        Metadata metadata = new Metadata();
        metadata.set(Metadata.CONTENT_TYPE, "image/x-jbig2");

        new ImageIOTranscodingOCRParser(ocr)
                .parse(new ByteArrayInputStream(pngBytes()), new DefaultHandler(), metadata, new ParseContext());

        // The OCR delegate (e.g. Tess4J) derives its image format from CONTENT_TYPE_PARSER_OVERRIDE,
        // so both it and CONTENT_TYPE must be set to the transcoded PNG type.
        assertThat(ocr.seenMetadata.get(Metadata.CONTENT_TYPE)).isEqualTo("image/png");
        assertThat(ocr.seenMetadata.get(CONTENT_TYPE_PARSER_OVERRIDE)).isEqualTo("image/ocr-png");
        // The delegate receives a valid, decodable PNG stream.
        assertThat(ImageIO.read(new ByteArrayInputStream(ocr.seenBytes))).isNotNull();
    }

    @Test
    public void testDecodesViaSniffingWhenContentTypeMissing() throws Exception {
        CapturingOcrParser ocr = new CapturingOcrParser();

        // No CONTENT_TYPE set: the parser must fall back to format-sniffing rather than emit nothing.
        new ImageIOTranscodingOCRParser(ocr)
                .parse(new ByteArrayInputStream(pngBytes()), new DefaultHandler(), new Metadata(), new ParseContext());

        assertThat(ocr.seenBytes).isNotNull();
        assertThat(ImageIO.read(new ByteArrayInputStream(ocr.seenBytes))).isNotNull();
    }
}
