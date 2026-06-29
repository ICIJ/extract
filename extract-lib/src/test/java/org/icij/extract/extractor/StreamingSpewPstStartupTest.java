package org.icij.extract.extractor;

import org.icij.extract.document.TikaDocument;
import org.icij.spewer.FieldNames;
import org.icij.spewer.Spewer;
import org.icij.task.Options;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.fest.assertions.Assertions.assertThat;

/**
 * Regression guard for the streaming-spew startup-ordering deadlock.
 *
 * <p>A PST/OST container emits NO root body text until the parse ends (all content is in embeds), so
 * {@code extract()}'s Tika first-character read of the root pipe blocks for the whole parse. The spew
 * worker MUST be started before that blocking read; otherwise embeds fill the bounded queue with no
 * consumer and, once the queue is full, the parse thread blocks on {@code queue.put()} and the parse
 * can never finish (the first-character read never returns) -> hard deadlock.
 *
 * <p>This reproduces the condition with a tiny {@code spewQueueCapacity} (2) and a PST containing far
 * more embeds than that. With the worker started before {@code extract()} the extraction completes and
 * every embed streams out; with the pre-fix ordering this test would hang and trip the JUnit timeout.
 */
public class StreamingSpewPstStartupTest {

    private static final String PST = "/documents/pst/testPST.pst";

    /** Reads each document's reader (mirroring a real spewer) and counts the writes. Thread-safe. */
    private static class CountingSpewer extends Spewer {
        final AtomicInteger docs = new AtomicInteger();
        CountingSpewer() { super(new FieldNames()); }
        @Override
        protected void writeDocument(TikaDocument doc, TikaDocument parent, TikaDocument root, int level) throws IOException {
            try { Spewer.toString(doc.getReader()); } catch (final Exception ignored) { /* count regardless */ }
            docs.incrementAndGet();
        }
    }

    @Test(timeout = 120_000)
    public void testPstStreamsWithTinyQueueWithoutDeadlock() throws Exception {
        final CountingSpewer spewer = new CountingSpewer();
        try (Extractor extractor = new Extractor(Options.from(Map.of(
                "streamingSpew", "true",
                "spewQueueCapacity", "2",          // far smaller than the PST's embed count
                "ocr", "false",                    // fast + deterministic, no Tesseract dependency
                "progressHeartbeatInterval", "0")))) {
            extractor.extract(Paths.get(getClass().getResource(PST).toURI()), spewer);
        }
        // testPST.pst has 7 messages plus attachments -> well past the 2-slot queue. That all of them
        // were written proves the worker drained the queue DURING the (blocking) parse: no deadlock.
        assertThat(spewer.docs.get()).isGreaterThan(2);
    }
}
