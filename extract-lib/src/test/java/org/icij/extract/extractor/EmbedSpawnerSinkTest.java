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

            // Deferred readies fire asynchronously when each embed's OCR future completes. Drain every
            // embed reader (which blocks on join(done) for that embed), ensuring all whenComplete
            // callbacks have fired before we assert counts.
            drainEmbedReaders(doc);

            assertThat(sink.promises.get()).isGreaterThan(0);
            // Every promise must eventually be matched by a ready.
            assertThat(sink.readyIds.size()).isEqualTo(sink.promises.get());
        }
    }

    // F3 regression: if writeEmbed throws (here: embedOutput points at a regular FILE, so resolving a
    // destination under it and copying fails for every embed), the promise() taken at embed creation
    // must still be balanced by a ready(). A leaked promise would hang StreamingSpewCoordinator
    // .awaitDrained() forever; the bounded timeout catches a regression as a failure, not a hang.
    @Test(timeout = 60_000)
    public void testFailedEmbedWriteStillBalancesPromise() throws Exception {
        final RecordingSink sink = new RecordingSink();
        // A regular file used as the "output directory": outputPath.resolve(hash) then Files.copy fails.
        final java.io.File notADir = java.io.File.createTempFile("embed-out-not-a-dir", ".tmp");
        notADir.deleteOnExit();

        try (Extractor extractor = new Extractor(Options.from(Map.of(
                "ocrParallelism", "4", "ocrFanout", "true", "progressHeartbeatInterval", "0",
                "embedOutput", notADir.getAbsolutePath())))) {

            final TikaDocument doc = extractor.extract(
                    Paths.get(getClass().getResource(FIXTURE).getPath()), sink);

            try (Reader r = doc.getReader()) {
                org.icij.spewer.Spewer.toString(r);
            }
            drainEmbedReaders(doc);

            assertThat(sink.promises.get()).isGreaterThan(0);
            // Every promise is matched by a ready even though every artifact write failed: no leaked promise.
            assertThat(sink.readyIds.size()).isEqualTo(sink.promises.get());
        }
    }

    private void drainEmbedReaders(org.icij.extract.document.TikaDocument doc) throws Exception {
        for (org.icij.extract.document.EmbeddedTikaDocument embed : doc.getEmbeds()) {
            try (java.io.Reader r = embed.getReader()) {
                org.icij.spewer.Spewer.toString(r);
            }
            drainEmbedReaders(embed);
        }
    }
}
