package org.icij.spewer;

import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.PathIdentifier;
import org.icij.extract.document.TikaDocument;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

import static org.fest.assertions.Assertions.assertThat;

public class StreamingSpewBackpressureTest {

    private static class SlowSpewer extends Spewer {
        final AtomicInteger writes = new AtomicInteger();
        SlowSpewer() { super(new FieldNames()); }
        @Override
        protected void writeDocument(TikaDocument doc, TikaDocument parent, TikaDocument root, int level) throws IOException {
            if (parent != null) {
                try { Thread.sleep(2); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                writes.incrementAndGet();
            }
        }
    }

    @Test(timeout = 30_000)
    public void testBoundedQueueDoesNotDeadlock() throws Exception {
        SlowSpewer spewer = new SlowSpewer();
        TikaDocument root = new DocumentFactory().withIdentifier(new PathIdentifier()).create(Paths.get("root"));
        root.setReader(new StringReader("root"));

        final int n = 200;
        try (StreamingSpewCoordinator coord = new StreamingSpewCoordinator(spewer, 4)) { // tiny bound
            // Promise all up front (non-blocking) so spew()'s awaitDrained waits for all n, mirroring the
            // real flow where every promise is made during the root-drive parse before awaitDrained runs.
            for (int i = 0; i < n; i++) {
                coord.promise();
            }
            // Producer readies 200 items into the 4-slot queue; ready() blocks when full (backpressure).
            Thread producer = new Thread(() -> {
                for (int i = 0; i < n; i++) {
                    TikaDocument e = new DocumentFactory().withIdentifier(new PathIdentifier()).create(Paths.get("e" + i));
                    e.setReader(new StringReader("x"));
                    coord.ready(new SpewItem(e, root, root, 1));
                }
            }, "test-producer");
            producer.start();
            // spew() starts the worker (drains concurrently with the producer), writes the root, then
            // awaits all n embeds. The producer unblocks as the worker drains -> no deadlock.
            coord.spew(root);
            producer.join();
        }
        assertThat(spewer.writes.get()).isEqualTo(n);
    }
}
