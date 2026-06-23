package org.icij.extract.ocr;

import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.junit.Test;

import java.util.Set;

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
}
