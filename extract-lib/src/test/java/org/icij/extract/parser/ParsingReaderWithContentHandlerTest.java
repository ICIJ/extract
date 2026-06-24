package org.icij.extract.parser;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.junit.Assert;
import org.junit.Test;
import org.xml.sax.ContentHandler;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.fest.assertions.Assertions.assertThat;

public class ParsingReaderWithContentHandlerTest {

    /**
     * When the TIKA-203 first-character read in the constructor fails (e.g. the consuming thread is
     * interrupted by a parse watchdog), the half-constructed reader must close its read end so the
     * background parse thread is unblocked on its next pipe write instead of leaking forever.
     */
    @Test(timeout = 10_000)
    public void testFirstCharReadFailureUnblocksParseThread() throws Exception {
        final CountDownLatch parseExited = new CountDownLatch(1);

        // A parser that floods the pipe and blocks on a full buffer (nothing drains the read end).
        final Parser flooding = new Parser() {
            @Override
            public Set<MediaType> getSupportedTypes(final ParseContext context) {
                return Set.of();
            }

            @Override
            public void parse(final InputStream stream, final ContentHandler handler, final Metadata metadata,
                              final ParseContext context) {
                try {
                    handler.startDocument();
                    final char[] block = new char[8192];
                    Arrays.fill(block, 'x');
                    // No reader drains the pipe, so this write blocks once the buffer fills, then
                    // throws as soon as the read end is closed by the failed constructor.
                    for (int i = 0; i < 100; i++) {
                        handler.characters(block, 0, block.length);
                    }
                } catch (final Throwable expected) {
                    // pipe closed -> write fails -> thread exits cleanly
                } finally {
                    parseExited.countDown();
                }
            }
        };

        // Pre-set the interrupt flag so the constructor's first-character read throws immediately.
        Thread.currentThread().interrupt();
        try {
            new ParsingReaderWithContentHandler(flooding, new ByteArrayInputStream("data".getBytes()),
                    new Metadata(), new ParseContext(), BodyContentHandler::new);
            Assert.fail("expected the interrupted first-character read to fail construction");
        } catch (final IOException expected) {
            // construction failed as intended
        } finally {
            // Clear the interrupt flag so it does not leak into other tests.
            Thread.interrupted();
        }

        // With the close-on-failure fix the parse thread's blocked write throws and the thread exits;
        // without it the thread stays blocked on the full pipe forever and this awaits to false.
        assertThat(parseExited.await(5, TimeUnit.SECONDS)).isTrue();
    }
}
