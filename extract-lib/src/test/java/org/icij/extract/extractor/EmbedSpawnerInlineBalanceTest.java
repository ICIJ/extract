package org.icij.extract.extractor;

import org.apache.tika.io.TemporaryResources;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.SecureContentHandler;
import org.icij.extract.document.DigestIdentifier;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.TikaDocument;
import org.junit.Test;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;

import static org.fest.assertions.Assertions.assertThat;

/**
 * EmbedSpawner overrides parseEmbedded rather than inheriting EmbedParser's, so it needs the same
 * balance guarantee on its own terms: its INLINE branch wraps the embed in a <div class="package-entry">
 * too, and Tika's SecureContentHandler pops that entry only on the matching endElement, counting across
 * the whole root parse. A skipped writeEnd therefore costs every later embed one level of nesting
 * budget. This path runs at Tika's DEFAULT limits (unlike the ARTIFACT walk, which relaxes them), where
 * the package-entry ceiling is 10, so a handful of failed inline embeds is enough to start refusing
 * embeds that are barely nested.
 */
public class EmbedSpawnerInlineBalanceTest {

    private static final String DELEGATE_FAILURE = "inline embed parse failed";

    private TikaDocument root() {
        return new DocumentFactory()
                .withIdentifier(new DigestIdentifier("SHA-256", Charset.defaultCharset()))
                .create(Paths.get("/tmp/root.pdf"));
    }

    // Serial spawner (no fan-out, no OCR, no spool) whose delegate parse always fails with a
    // SAXException -- the class delegateParsing's tolerant catch (TikaException) does not catch, so it
    // propagates out of parseEmbedded exactly as SecureSAXException does in production.
    private EmbedSpawner failingSpawner(final TikaDocument root) {
        return new EmbedSpawner(root, new ParseContext(), null,
                w -> new BodyContentHandler(w), 64L * 1024 * 1024, new TemporaryResources(),
                () -> false, EmbedSpawner.DEFAULT_MAX_EMBED_DEPTH) {
            @Override
            void delegateParsing(final InputStream input, final ContentHandler handler, final Metadata metadata)
                    throws SAXException {
                throw new SAXException(DELEGATE_FAILURE);
            }
        };
    }

    private Metadata inline(final String name) {
        final Metadata metadata = new Metadata();
        metadata.set(TikaCoreProperties.RESOURCE_NAME_KEY, name);
        metadata.set(TikaCoreProperties.EMBEDDED_RESOURCE_TYPE,
                TikaCoreProperties.EmbeddedResourceType.INLINE.toString());
        return metadata;
    }

    private InputStream embedBytes() {
        return new ByteArrayInputStream("inline".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void aFailedInlineEmbedDoesNotLeakPackageEntryDepthOntoItsSibling() throws Exception {
        // Tika's real accounting, with the package-entry limit lowered so a SINGLE leaked entry is
        // enough to refuse the next sibling (startElement throws once size() >= the limit).
        final SecureContentHandler secure =
                new SecureContentHandler(new DefaultHandler(), TikaInputStream.get(new byte[0]));
        secure.setMaximumPackageEntryDepth(2);
        final EmbedSpawner spawner = failingSpawner(root());

        // First inline embed fails. Propagating is correct; leaving its div open is not.
        try {
            spawner.parseEmbedded(embedBytes(), secure, inline("first.png"), true);
            throw new AssertionError("expected the delegate parse to fail");
        } catch (final SAXException expected) {
            assertThat(expected.getMessage()).isEqualTo(DELEGATE_FAILURE);
        }

        // The sibling must still be emittable, so the only failure it can report is its own delegate
        // failure. RED before the fix: writeStart throws SecureSAXException "2 levels of package entry
        // nesting" instead, because the first inline embed's entry was never popped.
        try {
            spawner.parseEmbedded(embedBytes(), secure, inline("second.png"), true);
            throw new AssertionError("expected the delegate parse to fail");
        } catch (final SAXException expected) {
            assertThat(expected.getMessage()).isEqualTo(DELEGATE_FAILURE);
        }
    }
}
