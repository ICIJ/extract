package org.icij.extract.extractor;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.BodyContentHandler;
import org.icij.extract.document.DigestIdentifier;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.TikaDocument;
import org.junit.Test;
import org.xml.sax.ContentHandler;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.fest.assertions.Assertions.assertThat;
import static org.fest.assertions.Fail.fail;

/**
 * {@link EmbedParser#delegateParsing} is the one place every embed-boundary parse funnels
 * through: EmbedSpawner's serial and deferred-OCR spawn paths call it, and so does the
 * ARTIFACT/download retrieval walk (EmbeddedDocumentExtractor, built directly on EmbedParser).
 * Unlike {@link EmbedSpawner#parseEmbedded} (see {@link EmbedSpawnerCancellationTest}), it had no
 * cooperative interrupt check of its own, so the retrieval path ignored Extractor's parse-timeout
 * {@code future.cancel(true)} interrupt and could hang a worker on a pathological entry (a
 * quadratic parser, a huge/malformed container, ...).
 */
public class EmbedParserCancellationTest {

    private static TikaDocument root() {
        return new DocumentFactory()
                .withIdentifier(new DigestIdentifier("SHA-256", Charset.defaultCharset()))
                .create(Paths.get("/tmp/fake-root"));
    }

    /** A delegate parser that records how many entries it actually got asked to parse. */
    private static class CountingParser extends AbstractParser {
        final AtomicInteger invocations = new AtomicInteger();

        @Override
        public Set<MediaType> getSupportedTypes(final ParseContext context) {
            return Collections.emptySet();
        }

        @Override
        public void parse(final InputStream stream, final ContentHandler handler,
                          final Metadata metadata, final ParseContext context) {
            invocations.incrementAndGet();
        }
    }

    private static Metadata nonInline(final String name) {
        final Metadata m = new Metadata();
        m.set(TikaCoreProperties.RESOURCE_NAME_KEY, name);
        return m;
    }

    private static InputStream payload() {
        return new ByteArrayInputStream("payload".getBytes(Charset.defaultCharset()));
    }

    @Test
    public void testInterruptedDelegateParsingAbortsBeforeParsingTheEntry() throws Exception {
        final CountingParser counting = new CountingParser();
        final EmbedParser parser = new EmbedParser(root(), new ParseContext(), counting);

        Thread.currentThread().interrupt(); // simulate future.cancel(true) on parse-timeout
        boolean threw = false;
        try {
            parser.delegateParsing(payload(), new BodyContentHandler(), nonInline("attachment.bin"));
        } catch (final InterruptedIOException expected) {
            threw = true;
            // The interrupt flag must stay set so every subsequent embed on this parse also aborts,
            // even if a Tika container parser swallows an individual throw.
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted(); // clear so we don't poison the test-runner thread
        }

        assertThat(threw).isTrue();
        // The entry's own content was never handed to the delegate parser: the walk aborted
        // before doing any work, exactly like EmbedSpawner's equivalent check.
        assertThat(counting.invocations.get()).isEqualTo(0);
    }

    @Test
    public void testInterruptAbortsBetweenEntriesNotOnlyTheFirst() throws Exception {
        // Simulates a multi-embed walk: the first entry parses normally, then a cancellation
        // fires mid-walk (Extractor's parse-timeout interrupting the thread), and the SECOND
        // entry must abort immediately instead of being parsed -- the "aborts between entries"
        // behavior the fix is meant to guarantee.
        final CountingParser counting = new CountingParser();
        final EmbedParser parser = new EmbedParser(root(), new ParseContext(), counting);

        parser.delegateParsing(payload(), new BodyContentHandler(), nonInline("entry-1.bin"));
        assertThat(counting.invocations.get()).isEqualTo(1);

        Thread.currentThread().interrupt();
        try {
            parser.delegateParsing(payload(), new BodyContentHandler(), nonInline("entry-2.bin"));
            fail("expected InterruptedIOException");
        } catch (final InterruptedIOException expected) {
            // expected
        } finally {
            Thread.interrupted(); // clear so we don't poison the test-runner thread
        }

        // Entry 2 was never handed to the delegate parser.
        assertThat(counting.invocations.get()).isEqualTo(1);
    }
}
