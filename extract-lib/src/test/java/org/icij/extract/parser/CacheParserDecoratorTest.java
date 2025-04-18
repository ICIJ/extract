package org.icij.extract.parser;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serial;
import java.io.StringWriter;
import java.io.Writer;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.WriteOutContentHandler;
import org.apache.tika.sax.XHTMLContentHandler;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

public class CacheParserDecoratorTest {

    private static final Path tmpDir = Paths.get(System.getProperty("java.io.tmpdir"), "tesseract-cache");

    private final URL simple = getClass().getResource("/documents/ocr/simple.tiff");

    @BeforeClass
    public static void setUp() throws IOException {
        if (!Files.isDirectory(tmpDir)) {
            Files.createDirectory(tmpDir);
        }
    }

    @AfterClass
    public static void cleanUp() throws IOException {
        Files.list(tmpDir).forEach(path -> {
            try {
                Files.delete(path);
            } catch (final IOException ignored) {
            }
        });
    }

    private static class ConstantParser implements Parser {
        public static final org.apache.tika.parser.EmptyParser INSTANCE = new org.apache.tika.parser.EmptyParser();
        private final String content;
        private final Set<MediaType> supportedTypes;

        public ConstantParser(Set<MediaType> supportedTypes, String content) {
            this.supportedTypes = supportedTypes;
            this.content = content;
        }

        public Set<MediaType> getSupportedTypes(ParseContext context) {
            return supportedTypes;
        }

        public void parse(InputStream stream, ContentHandler handler, Metadata metadata, ParseContext context) throws
            SAXException {
            XHTMLContentHandler xhtml = new XHTMLContentHandler(handler, metadata);
            xhtml.characters(content);
            xhtml.startDocument();
            xhtml.endDocument();
        }
    }


    @Test
    public void testWriteToCache() throws Throwable {
        final Path simple = Paths.get(Objects.requireNonNull(this.simple).toURI());

        Writer writer = new StringWriter();
        final AtomicInteger hit = new AtomicInteger(), miss = new AtomicInteger();
        final String expectedParsing = "some_parsing";

        Parser inner = new ConstantParser(Set.of(MediaType.image("tiff")), expectedParsing);
        final CacheParserDecorator decorated = new CacheParserDecorator(inner, tmpDir) {

            @Serial
            private static final long serialVersionUID = 6551690243986921730L;

            @Override
            public void cacheHit() {
                hit.incrementAndGet();
            }

            @Override
            public void cacheMiss() {
                miss.incrementAndGet();
            }
        };

        try (final InputStream in = Files.newInputStream(simple)) {
            decorated.parse(in, new WriteOutContentHandler(writer), new Metadata(), new ParseContext());
        }

        Assert.assertEquals(expectedParsing, writer.toString().trim());
        Assert.assertEquals(0, hit.get());
        Assert.assertEquals(1, miss.get());

        // Try again from the cache.
        writer = new StringWriter();
        try (final InputStream in = Files.newInputStream(simple)) {
            decorated.parse(in, new WriteOutContentHandler(writer), new Metadata(), new ParseContext());
        }

        Assert.assertEquals(expectedParsing, writer.toString().trim());
        Assert.assertEquals(1, hit.get());
        Assert.assertEquals(1, miss.get());
    }
}
