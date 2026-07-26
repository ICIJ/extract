package org.icij.extract.extractor;

import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.SecureContentHandler;
import org.icij.extract.document.DigestIdentifier;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.TikaDocument;
import org.junit.Test;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.fest.assertions.Assertions.assertThat;

/**
 * EmbedParser wraps every emitted embed in a nested <div class="package-entry">, and Tika's
 * SecureContentHandler counts those divs across the WHOLE root parse, popping an entry only on the
 * matching endElement. A per-embed parse failure that skips writeEnd therefore leaves the entry on
 * the stack forever, and since callers of this walk tolerate per-entry failures and keep going, the
 * nesting count climbs with every failure until the guard refuses embeds that are not nested at all.
 * This pins that one failed embed cannot cost its siblings any nesting budget.
 */
public class EmbedParserPackageEntryBalanceTest {

    private static final String DELEGATE_FAILURE = "embed parse failed";

    // Fails the way the zip-bomb guard fails: with a SAXException, which delegateParsing's tolerant
    // catch (TikaException) deliberately does not catch, so it propagates out of parseEmbedded.
    private static class FailingEmbedParser extends EmbedParser {
        FailingEmbedParser(final TikaDocument root, final ParseContext context) {
            super(root, context);
        }

        @Override
        void delegateParsing(final InputStream input, final ContentHandler handler, final Metadata metadata)
                throws SAXException {
            throw new SAXException(DELEGATE_FAILURE);
        }
    }

    private Metadata named(final String name) {
        final Metadata metadata = new Metadata();
        metadata.set(TikaCoreProperties.RESOURCE_NAME_KEY, name);
        return metadata;
    }

    private InputStream entryBytes() {
        return new ByteArrayInputStream("entry".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void aFailedEmbedDoesNotLeakPackageEntryDepthOntoItsSibling() throws Exception {
        // Tika's real accounting, with the package-entry limit lowered so a SINGLE leaked entry is
        // enough to refuse the next sibling (startElement throws once size() >= the limit).
        final SecureContentHandler secure =
                new SecureContentHandler(new DefaultHandler(), TikaInputStream.get(new byte[0]));
        secure.setMaximumPackageEntryDepth(2);
        final TikaDocument root = new DocumentFactory()
                .withIdentifier(new DigestIdentifier("SHA-256", Charset.defaultCharset()))
                .create(Paths.get("/tmp/root.zip"));
        final EmbedParser parser = new FailingEmbedParser(root, new ParseContext());

        // First embed fails. Propagating is correct (the caller decides whether to keep walking), but
        // it must not leave this embed's package-entry div open.
        try {
            parser.parseEmbedded(entryBytes(), secure, named("first.txt"), true);
            throw new AssertionError("expected the delegate parse to fail");
        } catch (final SAXException expected) {
            assertThat(expected.getMessage()).isEqualTo(DELEGATE_FAILURE);
        }

        // The sibling must still be emittable, so the only failure it can report is its own delegate
        // failure. RED before the fix: writeStart throws SecureSAXException "2 levels of package entry
        // nesting" instead, because the first embed's entry was never popped.
        try {
            parser.parseEmbedded(entryBytes(), secure, named("second.txt"), true);
            throw new AssertionError("expected the delegate parse to fail");
        } catch (final SAXException expected) {
            assertThat(expected.getMessage()).isEqualTo(DELEGATE_FAILURE);
        }
    }

    // Records the elements it receives and refuses characters, so writeStart fails PART WAY: the
    // package-entry div's startElement has already reached the handler when the name emission throws.
    private static class RejectsCharacters extends DefaultHandler {
        static final String REJECTED = "handler rejected characters";
        final List<String> events = new ArrayList<>();

        @Override
        public void startElement(final String uri, final String localName, final String qName, final Attributes atts) {
            events.add("start:" + qName);
        }

        @Override
        public void endElement(final String uri, final String localName, final String qName) {
            events.add("end:" + qName);
        }

        @Override
        public void characters(final char[] ch, final int start, final int length) throws SAXException {
            throw new SAXException(REJECTED);
        }
    }

    @Test
    public void writeStartClosesItsOwnDivWhenTheNameEmissionThrows() throws Exception {
        // parseEmbedded's finally cannot help here: writeStart is deliberately outside its try (an
        // endElement for a startElement that never reached the handler would be unmatched), so if
        // writeStart itself throws after opening the div, only writeStart can close it. The live
        // compression-ratio guard makes this reachable: SecureContentHandler.advance throws from
        // characters(), which is exactly where writeStart emits the resource name.
        final RejectsCharacters handler = new RejectsCharacters();
        final TikaDocument root = new DocumentFactory()
                .withIdentifier(new DigestIdentifier("SHA-256", Charset.defaultCharset()))
                .create(Paths.get("/tmp/root.zip"));
        final EmbedParser parser = new FailingEmbedParser(root, new ParseContext());

        try {
            parser.parseEmbedded(entryBytes(), handler, named("first.txt"), true);
            throw new AssertionError("expected the name emission to fail");
        } catch (final SAXException expected) {
            assertThat(expected.getMessage()).isEqualTo(RejectsCharacters.REJECTED);
        }

        // RED before the fix: events are [start:div, start:h1] and the div stays open forever.
        assertThat(handler.events).contains("end:div");
    }
}
