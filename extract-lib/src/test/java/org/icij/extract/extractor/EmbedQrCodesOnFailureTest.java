package org.icij.extract.extractor;

import static org.fest.assertions.Assertions.assertThat;
import static org.icij.extract.parser.QRCodeParserDecorator.QR_CODES;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import org.apache.tika.io.TemporaryResources;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.BodyContentHandler;
import org.icij.extract.document.DigestIdentifier;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.TikaDocument;
import org.junit.Test;
import org.xml.sax.ContentHandler;

public class EmbedQrCodesOnFailureTest {
    private static final String VALUE = "https://example.org/qr/one";

    @Test
    public void test_concatenated_embed_values_reach_the_parent_when_the_parse_throws_an_error() throws Exception {
        TikaDocument root = root();
        EmbedParser parser = new EmbedParser(root, new ParseContext(), null) {
            @Override
            void delegateParsing(InputStream input, ContentHandler handler, Metadata metadata) {
                decodeThenCrash(metadata);
            }
        };

        parseExpectingError(() -> parser.parseEmbedded(bytes(), new BodyContentHandler(-1), inline(), true));

        assertThat(root.getMetadata().getValues(QR_CODES)).containsOnly(VALUE);
    }

    @Test
    public void test_inline_spawned_embed_values_reach_the_parent_when_the_parse_throws_an_error() throws Exception {
        TikaDocument root = root();
        EmbedSpawner spawner = new EmbedSpawner(root, new ParseContext(), null,
                BodyContentHandler::new, 64L * 1024 * 1024, new TemporaryResources(),
                () -> false, EmbedSpawner.DEFAULT_MAX_EMBED_DEPTH) {
            @Override
            void delegateParsing(InputStream input, ContentHandler handler, Metadata metadata) {
                decodeThenCrash(metadata);
            }
        };

        parseExpectingError(() -> spawner.parseEmbedded(bytes(), new BodyContentHandler(-1), inline(), true));

        assertThat(root.getMetadata().getValues(QR_CODES)).containsOnly(VALUE);
    }

    private interface EmbedParse {
        void run() throws Exception;
    }

    private static void parseExpectingError(EmbedParse parse) throws Exception {
        try {
            parse.run();
            fail("the parse failure must still reach the caller");
        } catch (ParserCrash expected) {
            // the values were decoded before the crash
        }
    }

    private static void decodeThenCrash(Metadata metadata) {
        metadata.add(QR_CODES, VALUE);
        throw new ParserCrash();
    }

    private static class ParserCrash extends Error {
    }

    private static TikaDocument root() {
        return new DocumentFactory()
                .withIdentifier(new DigestIdentifier("SHA-256", Charset.defaultCharset()))
                .create(Paths.get("/tmp/root.pdf"));
    }

    private static Metadata inline() {
        Metadata metadata = new Metadata();
        metadata.set(TikaCoreProperties.RESOURCE_NAME_KEY, "image0.png");
        metadata.set(TikaCoreProperties.EMBEDDED_RESOURCE_TYPE, TikaCoreProperties.EmbeddedResourceType.INLINE.toString());
        return metadata;
    }

    private static InputStream bytes() {
        return new ByteArrayInputStream("png".getBytes(StandardCharsets.UTF_8));
    }
}
