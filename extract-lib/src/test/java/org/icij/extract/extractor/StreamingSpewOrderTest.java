package org.icij.extract.extractor;

import org.icij.extract.document.TikaDocument;
import org.icij.spewer.FieldNames;
import org.icij.spewer.Spewer;
import org.icij.task.Options;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.fest.assertions.Assertions.assertThat;

public class StreamingSpewOrderTest {

    private static final String FIXTURE = "/documents/image_attachment.eml";

    // Thread-safe recorder: notes the order of writes and whether each was a child (parent != null).
    private static class OrderRecordingSpewer extends Spewer {
        final List<String> order = new CopyOnWriteArrayList<>();
        OrderRecordingSpewer() { super(new FieldNames()); }
        @Override
        protected void writeDocument(TikaDocument doc, TikaDocument parent, TikaDocument root, int level) throws IOException {
            try { Spewer.toString(doc.getReader()); } catch (Exception ignored) {}
            order.add((parent == null ? "ROOT:" : "EMBED:") + doc.getId());
        }
    }

    @Test
    public void testEmbedsAreWrittenAndRootIsCovered() throws Exception {
        OrderRecordingSpewer spewer = new OrderRecordingSpewer();
        try (Extractor extractor = new Extractor(Options.from(Map.of(
                "streamingSpew", "true",
                "ocrParallelism", "4", "ocrFanout", "true", "progressHeartbeatInterval", "0")))) {
            extractor.extract(Paths.get(getClass().getResource(FIXTURE).getPath()), spewer);
        }
        // At least one embed and the root were written.
        assertThat(spewer.order).isNotEmpty();
        assertThat(spewer.order.stream().anyMatch(s -> s.startsWith("ROOT:"))).isTrue();
        assertThat(spewer.order.stream().anyMatch(s -> s.startsWith("EMBED:"))).isTrue();
    }

    @Test
    public void testStreamingAndLegacyWriteTheSameDocIds() throws Exception {
        List<String> streaming = idsFor(true);
        List<String> legacy = idsFor(false);
        Collections.sort(streaming);
        Collections.sort(legacy);
        assertThat(streaming).isEqualTo(legacy);
    }

    private List<String> idsFor(boolean streaming) throws Exception {
        OrderRecordingSpewer spewer = new OrderRecordingSpewer();
        try (Extractor extractor = new Extractor(Options.from(Map.of(
                "streamingSpew", String.valueOf(streaming),
                "ocrParallelism", "4", "ocrFanout", "true", "progressHeartbeatInterval", "0")))) {
            extractor.extract(Paths.get(getClass().getResource(FIXTURE).getPath()), spewer);
        }
        List<String> ids = new java.util.ArrayList<>();
        for (String s : spewer.order) { ids.add(s.substring(s.indexOf(':') + 1)); }
        return ids;
    }
}
