package org.icij.spewer;

import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.PathIdentifier;
import org.icij.extract.document.TikaDocument;
import org.junit.Test;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.fest.assertions.Assertions.assertThat;

public class StreamingSpewCoordinatorTest {

    // A thread-safe recording spewer: records each written doc id and whether the root was a child write.
    private static class RecordingSpewer extends Spewer {
        final List<String> written = Collections.synchronizedList(new ArrayList<>());
        volatile boolean throwOnEmbed = false;

        RecordingSpewer() { super(new FieldNames()); }

        @Override
        protected void writeDocument(TikaDocument doc, TikaDocument parent, TikaDocument root, int level) throws IOException {
            if (throwOnEmbed && parent != null) {
                throw new IOException("boom writing " + doc.getId());
            }
            written.add(doc.getId());
        }
    }

    private TikaDocument doc(String id, Reader reader) {
        TikaDocument d = new DocumentFactory().withIdentifier(new PathIdentifier()).create(Paths.get(id));
        d.setReader(reader);
        return d;
    }

    @Test
    public void testWritesEveryPromisedEmbedAndTheRoot() throws Exception {
        RecordingSpewer spewer = new RecordingSpewer();
        TikaDocument root = doc("root", new StringReader("root-body"));
        TikaDocument e1 = doc("e1", new StringReader("a"));
        TikaDocument e2 = doc("e2", new StringReader("b"));

        try (StreamingSpewCoordinator coord = new StreamingSpewCoordinator(spewer, 16)) {
            // Simulate the parse: promise + ready each embed BEFORE the foreground writes the root.
            coord.promise();
            coord.ready(new SpewItem(e1, root, root, 1));
            coord.promise();
            coord.ready(new SpewItem(e2, root, root, 1));

            coord.spew(root); // foreground writes root, then awaits the two embeds
        }

        // Root and both embeds written; the two embeds before await completes.
        assertThat(spewer.written).contains("root", "e1", "e2");
        assertThat(spewer.written).hasSize(3);
    }

    @Test
    public void testSkipsChildrenWhenRootIsDuplicate() throws Exception {
        RecordingSpewer spewer = new RecordingSpewer();
        TikaDocument root = doc("root", new StringReader("root-body"));
        root.setDuplicate(true); // foreground's writeDocument(root) would set this in production
        TikaDocument e1 = doc("e1", new StringReader("a"));

        try (StreamingSpewCoordinator coord = new StreamingSpewCoordinator(spewer, 16)) {
            coord.promise();
            coord.ready(new SpewItem(e1, root, root, 1));
            coord.spew(root);
        }

        // The root is still written (its writeDocument runs); the child is skipped.
        assertThat(spewer.written).contains("root");
        assertThat(spewer.written).excludes("e1");
    }

    @Test
    public void testWorkerErrorIsRethrownFromSpew() throws Exception {
        RecordingSpewer spewer = new RecordingSpewer();
        spewer.throwOnEmbed = true;
        TikaDocument root = doc("root", new StringReader("root-body"));
        TikaDocument e1 = doc("e1", new StringReader("a"));

        try (StreamingSpewCoordinator coord = new StreamingSpewCoordinator(spewer, 16)) {
            coord.promise();
            coord.ready(new SpewItem(e1, root, root, 1));
            try {
                coord.spew(root);
                org.junit.Assert.fail("expected the worker's IOException to propagate");
            } catch (IOException expected) {
                assertThat(expected.getMessage()).contains("boom");
            }
        }
    }
}
