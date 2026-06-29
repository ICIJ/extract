package org.icij.extract.extractor;

import org.icij.extract.document.TikaDocument;
import org.icij.spewer.SpewItem;
import org.icij.spewer.SpewSink;
import org.icij.task.Options;
import org.junit.Test;

import java.io.Reader;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.fest.assertions.Assertions.assertThat;

public class EmbedSpawnerSinkTest {

    private static final String FIXTURE = "/documents/image_attachment.eml";

    // Records promise/ready counts and the ready embed ids, thread-safely (deferred ready arrives off-thread).
    private static class RecordingSink implements SpewSink {
        final AtomicInteger promises = new AtomicInteger();
        final ConcurrentLinkedQueue<String> readyIds = new ConcurrentLinkedQueue<>();
        @Override public void promise() { promises.incrementAndGet(); }
        @Override public void ready(SpewItem item) { readyIds.add(item.embed().getId()); }
    }

    @Test
    public void testSinkPromisesAndReadiesEachEmbed() throws Exception {
        RecordingSink sink = new RecordingSink();
        try (Extractor extractor = new Extractor(Options.from(Map.of(
                "ocrParallelism", "4", "ocrFanout", "true", "progressHeartbeatInterval", "0")))) {

            TikaDocument doc = extractor.extract(
                    Paths.get(getClass().getResource(FIXTURE).getPath()), sink);

            // Draining the root reader drives the parse + the deferred OCR backstop, so every embed
            // is promised (parse thread) and readied (OCR completion) by the time the reader EOFs.
            try (Reader r = doc.getReader()) {
                org.icij.spewer.Spewer.toString(r);
            }

            assertThat(sink.promises.get()).isGreaterThan(0);
            // Every promise must eventually be matched by a ready.
            assertThat(sink.readyIds.size()).isEqualTo(sink.promises.get());
        }
    }
}
