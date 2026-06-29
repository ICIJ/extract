package org.icij.extract.extractor;

import org.icij.extract.document.TikaDocument;
import org.icij.spewer.FieldNames;
import org.icij.spewer.Spewer;
import org.icij.task.Options;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.fest.assertions.Assertions.assertThat;

public class StreamingSpewFailureTest {

    private static final String FIXTURE = "/documents/image_attachment.eml";

    // Spewer that throws on the root write (parent == null) but records any embed writes first.
    private static class FailOnRootSpewer extends Spewer {
        final List<String> embedsWritten = new CopyOnWriteArrayList<>();
        FailOnRootSpewer() { super(new FieldNames()); }
        @Override
        protected void writeDocument(TikaDocument doc, TikaDocument parent, TikaDocument root, int level) throws IOException {
            if (parent == null) {
                // Drain the pipe first so embeds get produced and streamed, THEN fail the root write.
                try { Spewer.toString(doc.getReader()); } catch (Exception ignored) {}
                throw new IOException("simulated root write failure");
            }
            embedsWritten.add(doc.getId());
        }
    }

    @Test
    public void testRootFailurePropagatesAndEmbedsAlreadyWrittenStay() throws Exception {
        FailOnRootSpewer spewer = new FailOnRootSpewer();
        try (Extractor extractor = new Extractor(Options.from(Map.of(
                "streamingSpew", "true",
                "ocrParallelism", "4", "ocrFanout", "true", "progressHeartbeatInterval", "0")))) {

            try {
                extractor.extract(Paths.get(getClass().getResource(FIXTURE).getPath()), spewer);
                org.junit.Assert.fail("expected the root write failure to propagate");
            } catch (IOException expected) {
                assertThat(expected.getMessage()).contains("simulated root write failure");
            }
        }
        // The embeds streamed during the parse were written before the root write failed.
        assertThat(spewer.embedsWritten).isNotEmpty();
    }
}
