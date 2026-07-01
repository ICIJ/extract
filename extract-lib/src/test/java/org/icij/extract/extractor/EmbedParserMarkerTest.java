package org.icij.extract.extractor;

import org.apache.tika.exception.EncryptedDocumentException;
import org.apache.tika.exception.TikaException;
import org.apache.tika.exception.ZeroByteFileException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.icij.extract.document.DigestIdentifier;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.TikaDocument;
import org.junit.Test;
import org.xml.sax.ContentHandler;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Set;

import static org.fest.assertions.Assertions.assertThat;

public class EmbedParserMarkerTest {

    private static TikaDocument root() {
        return new DocumentFactory()
                .withIdentifier(new DigestIdentifier("SHA-256", Charset.defaultCharset()))
                .create(Paths.get("/tmp/fake-root"));
    }

    /** A delegate parser that always throws the supplied exception. */
    private static Parser throwing(final TikaException toThrow) {
        return new AbstractParser() {
            @Override
            public Set<MediaType> getSupportedTypes(final ParseContext context) {
                return Collections.emptySet();
            }

            @Override
            public void parse(final InputStream stream, final ContentHandler handler,
                              final Metadata metadata, final ParseContext context) throws TikaException {
                throw toThrow;
            }
        };
    }

    private static Metadata parseWith(final TikaException toThrow) throws Exception {
        final Metadata metadata = new Metadata();
        final EmbedParser parser = new EmbedParser(root(), new ParseContext(), throwing(toThrow));
        parser.delegateParsing(new ByteArrayInputStream(new byte[]{1}), new BodyContentHandler(), metadata);
        return metadata;
    }

    @Test
    public void testEncryptedIsMarkedWithoutStacktrace() throws Exception {
        Metadata metadata = parseWith(new EncryptedDocumentException());
        assertThat(metadata.get(NoContentReason.METADATA_KEY)).isEqualTo("encrypted");
        assertThat(metadata.get(TikaCoreProperties.TIKA_META_EXCEPTION_EMBEDDED_STREAM)).isNull();
    }

    @Test
    public void testZeroByteIsMarkedWithoutStacktrace() throws Exception {
        Metadata metadata = parseWith(new ZeroByteFileException("empty"));
        assertThat(metadata.get(NoContentReason.METADATA_KEY)).isEqualTo("empty-file");
        assertThat(metadata.get(TikaCoreProperties.TIKA_META_EXCEPTION_EMBEDDED_STREAM)).isNull();
    }

    @Test
    public void testGenuineParseFailureStillRecordsException() throws Exception {
        Metadata metadata = parseWith(new TikaException("boom"));
        assertThat(metadata.get(NoContentReason.METADATA_KEY)).isNull();
        assertThat(metadata.get(TikaCoreProperties.TIKA_META_EXCEPTION_EMBEDDED_STREAM)).isNotNull();
    }
}
