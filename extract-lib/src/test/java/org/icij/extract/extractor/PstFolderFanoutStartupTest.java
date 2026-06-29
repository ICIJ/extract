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

public class PstFolderFanoutStartupTest {

    private static final String PST = "/documents/pst/testPST.pst";

    private static class CountingSpewer extends Spewer {
        final AtomicInteger docs = new AtomicInteger();
        CountingSpewer() { super(new FieldNames()); }
        @Override
        protected void writeDocument(TikaDocument doc, TikaDocument parent, TikaDocument root, int level) throws IOException {
            try { Spewer.toString(doc.getReader()); } catch (final Exception ignored) { }
            docs.incrementAndGet();
        }
    }

    @Test(timeout = 120_000)
    public void testFanoutStreamsWithTinyQueueWithoutDeadlock() throws Exception {
        final CountingSpewer spewer = new CountingSpewer();
        try (Extractor extractor = new Extractor(Options.from(Map.of(
                "pstFolderFanout", "true",
                "pstParseParallelism", "4",
                "streamingSpew", "true",
                "spewQueueCapacity", "2",   // far smaller than the PST's embed count
                "ocr", "false",
                "progressHeartbeatInterval", "0")))) {
            extractor.extract(Paths.get(getClass().getResource(PST).toURI()), spewer);
        }
        // The timeout is the primary deadlock guard; this count is a secondary liveness floor set
        // comfortably above spewQueueCapacity (2) to prove the worker drained many items DURING the
        // parse. testPST.pst has 7 messages plus attachments, well above this floor.
        assertThat(spewer.docs.get()).isGreaterThan(5);
    }
}
