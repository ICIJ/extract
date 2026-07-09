package org.icij.extract.extractor;

import org.icij.extract.document.TikaDocument;
import org.icij.spewer.FieldNames;
import org.icij.spewer.Spewer;
import org.icij.task.Options;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.fest.assertions.Assertions.assertThat;

/**
 * Streaming spew must not retain the parsed embed tree in memory: every embed is written to the
 * spewer as it is produced, and the streaming path never re-walks {@code root.getEmbeds()}. Keeping
 * the embeds linked under their parent grows the heap O(n) with the number of embedded documents,
 * which is what OOMs a large container (a PST/OST with hundreds of thousands of items).
 */
public class StreamingEmbedRetentionTest {

    private static final String FIXTURE = "/documents/image_attachment.eml";

    /** Captures the root document handed to the spewer so we can inspect its embed list afterwards. */
    private static class RootCapturingSpewer extends Spewer {
        final AtomicReference<TikaDocument> root = new AtomicReference<>();
        RootCapturingSpewer() { super(new FieldNames()); }
        @Override
        protected void writeDocument(TikaDocument doc, TikaDocument parent, TikaDocument r, int level) throws IOException {
            try { Spewer.toString(doc.getReader()); } catch (Exception ignored) {}
            if (parent == null) {
                root.set(doc);
            }
        }
    }

    @Test
    public void testStreamingDoesNotRetainEmbedsOnRoot() throws Exception {
        RootCapturingSpewer spewer = new RootCapturingSpewer();
        try (Extractor extractor = new Extractor(Options.from(Map.of(
                "streamingSpew", "true",
                "ocrParallelism", "4", "ocrFanout", "true", "progressHeartbeatInterval", "0")))) {
            extractor.extract(Paths.get(getClass().getResource(FIXTURE).getPath()), spewer);
        }
        assertThat(spewer.root.get()).isNotNull();
        // The fixture has at least one embedded document; streaming must have written it without
        // keeping it linked under the root, so the retained tree is empty once the parse ends.
        assertThat(spewer.root.get().hasEmbeds()).isFalse();
    }
}
