package org.icij.extract.extractor;

import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.ToTextContentHandler;
import org.apache.tika.sax.XHTMLContentHandler;
import org.icij.extract.document.DigestIdentifier;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.TikaDocument;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Set;

import static org.fest.assertions.Assertions.assertThat;

public class EmbedParserTextFallbackTest {

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    private static TikaDocument root() {
        return new DocumentFactory()
                .withIdentifier(new DigestIdentifier("SHA-256", Charset.defaultCharset()))
                .create(Paths.get("/tmp/fake-root"));
    }

    /** A delegate parser that emits the supplied text, then throws. */
    private static Parser throwing(final String textBeforeFailure) {
        return new AbstractParser() {
            @Override
            public Set<MediaType> getSupportedTypes(final ParseContext context) {
                return Collections.emptySet();
            }

            @Override
            public void parse(final InputStream stream, final ContentHandler handler,
                              final Metadata metadata, final ParseContext context)
                    throws TikaException, SAXException {
                handler.characters(textBeforeFailure.toCharArray(), 0, textBeforeFailure.length());
                throw new TikaException("malformed entry");
            }
        };
    }

    /** A delegate parser that emits Tika's XHTML preamble, which echoes the metadata title, then throws. */
    private static Parser throwingAfterXhtmlPreamble() {
        return new AbstractParser() {
            @Override
            public Set<MediaType> getSupportedTypes(final ParseContext context) {
                return Collections.emptySet();
            }

            @Override
            public void parse(final InputStream stream, final ContentHandler handler,
                              final Metadata metadata, final ParseContext context)
                    throws TikaException, SAXException {
                final XHTMLContentHandler xhtml = new XHTMLContentHandler(handler, metadata);
                xhtml.startDocument();
                xhtml.startElement("p");
                throw new TikaException("malformed entry");
            }
        };
    }

    private static Metadata named(final String name, final String type) {
        final Metadata metadata = new Metadata();
        metadata.set(TikaCoreProperties.RESOURCE_NAME_KEY, name);
        metadata.set(Metadata.CONTENT_TYPE, type);
        return metadata;
    }

    private String textOf(final InputStream entry, final Metadata metadata) throws Exception {
        final BodyContentHandler handler = new BodyContentHandler();
        new EmbedParser(root(), new ParseContext(), throwing("")).delegateParsing(entry, handler, metadata);
        return handler.toString();
    }

    private TikaInputStream spooled(final String name, final byte[] bytes) throws Exception {
        final Path file = tmp.newFile(name).toPath();
        Files.write(file, bytes);
        return TikaInputStream.get(file);
    }

    @Test
    public void testTextEntryMisroutedToXmlParserRecoversItsText() throws Exception {
        final byte[] source = "fn main() { let answer = 42; }\n".getBytes();
        try (TikaInputStream entry = spooled("sample.rs", source)) {
            assertThat(textOf(entry, named("sample.rs", "application/rls-services+xml")))
                    .contains("let answer = 42");
        }
    }

    @Test
    public void testMalformedXmlEntryRecoversItsText() throws Exception {
        final byte[] malformed = "<?xml version=\"1.0\"?><root><unclosed>marker text\n".getBytes();
        try (TikaInputStream entry = spooled("template.ctp", malformed)) {
            assertThat(textOf(entry, named("template.ctp", "application/xml"))).contains("marker text");
        }
    }

    @Test
    public void testBinaryEntryIsNotReparsedAsText() throws Exception {
        final byte[] binary = new byte[]{0x08, 0x01, 0x12, 0x00, 0x1a, (byte) 0xff, 0x00, 0x03, (byte) 0xfe};
        try (TikaInputStream entry = spooled("resources.arsc", binary)) {
            assertThat(textOf(entry, named("resources.arsc", "application/xml"))).isEmpty();
        }
    }

    @Test
    public void testEntryWithoutSpooledBytesIsNotRecovered() throws Exception {
        final InputStream entry = new ByteArrayInputStream("plain text entry\n".getBytes());
        assertThat(textOf(entry, named("notes.res", "application/xml"))).isEmpty();
    }

    @Test
    public void testEntryWhoseFailedParseAlreadyWroteTextIsNotReparsed() throws Exception {
        final byte[] malformed = "<?xml version=\"1.0\"?><root>partial<unclosed>\n".getBytes();
        final ToTextContentHandler handler = new ToTextContentHandler();

        try (TikaInputStream entry = spooled("partial.ctp", malformed)) {
            new EmbedParser(root(), new ParseContext(), throwing("partial"))
                    .delegateParsing(entry, handler, named("partial.ctp", "application/xml"));
        }

        assertThat(handler.toString()).isEqualTo("partial");
    }

    @Test
    public void testEntryWhoseFailedParseOnlyEchoedTheMetadataTitleIsRecovered() throws Exception {
        final Metadata metadata = named("titled.rs", "application/rls-services+xml");
        metadata.set(TikaCoreProperties.TITLE, "a title supplied by the container");
        final BodyContentHandler handler = new BodyContentHandler();

        try (TikaInputStream entry = spooled("titled.rs", "fn main() { let answer = 42; }\n".getBytes())) {
            new EmbedParser(root(), new ParseContext(), throwingAfterXhtmlPreamble())
                    .delegateParsing(entry, handler, metadata);
        }

        assertThat(handler.toString()).contains("let answer = 42");
    }
}
