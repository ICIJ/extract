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
        final List<String> rootStubs = Collections.synchronizedList(new ArrayList<>());
        final List<Long> rootStubChildCounts = Collections.synchronizedList(new ArrayList<>());
        volatile boolean throwOnEmbed = false;
        volatile String failOnId = null;

        RecordingSpewer() { super(new FieldNames()); }

        @Override
        protected void writeDocument(TikaDocument doc, TikaDocument parent, TikaDocument root, int level) throws IOException {
            if (throwOnEmbed && parent != null) {
                throw new IOException("boom writing " + doc.getId());
            }
            if (failOnId != null && failOnId.equals(doc.getId())) {
                throw new IOException("boom writing specific ID " + doc.getId());
            }
            written.add(doc.getId());
        }

        volatile boolean rootStubExists = false; // simulate a root already indexed (reindex): stub write is a no-op

        @Override
        protected boolean writeRootStub(TikaDocument root, long writtenChildren) {
            rootStubs.add(root.getId());
            rootStubChildCounts.add(writtenChildren);
            return !rootStubExists;
        }

        final List<String> finalizedRoots = Collections.synchronizedList(new ArrayList<>());
        final List<Long> finalizeChildCounts = Collections.synchronizedList(new ArrayList<>());

        @Override
        protected void finalizeRoot(TikaDocument root, long writtenChildren) {
            finalizedRoots.add(root.getId());
            finalizeChildCounts.add(writtenChildren);
        }

        final List<String> finalizedAbortedRoots = Collections.synchronizedList(new ArrayList<>());
        final List<Long> finalizeAbortedChildCounts = Collections.synchronizedList(new ArrayList<>());

        @Override
        protected void finalizeAbortedRoot(TikaDocument root, long writtenChildren) {
            finalizedAbortedRoots.add(root.getId());
            finalizeAbortedChildCounts.add(writtenChildren);
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
    public void testFinalizesRootWithChildCountOnNormalCompletion() throws Exception {
        RecordingSpewer spewer = new RecordingSpewer();
        TikaDocument root = doc("root", new StringReader("root-body"));
        TikaDocument e1 = doc("e1", new StringReader("a"));
        TikaDocument e2 = doc("e2", new StringReader("b"));

        try (StreamingSpewCoordinator coord = new StreamingSpewCoordinator(spewer, 16)) {
            coord.promise();
            coord.ready(new SpewItem(e1, root, root, 1));
            coord.promise();
            coord.ready(new SpewItem(e2, root, root, 1));
            coord.spew(root);
        }

        // Root written normally + it is a container (2 children written): finalize once with the count.
        assertThat(spewer.written).contains("root", "e1", "e2");
        assertThat(spewer.finalizedRoots).containsOnly("root");
        assertThat(spewer.finalizeChildCounts).containsOnly(2L);
        // The normal path never writes a stub.
        assertThat(spewer.rootStubs).isEmpty();
    }

    @Test
    public void testDoesNotFinalizeRootWhenCancelledMidStream() throws Exception {
        RecordingSpewer spewer = new RecordingSpewer();
        TikaDocument root = doc("root", new StringReader("root-body"));
        TikaDocument e1 = doc("e1", new StringReader("a"));

        try (StreamingSpewCoordinator coord = new StreamingSpewCoordinator(spewer, 16)) {
            coord.start();
            coord.promise();
            coord.ready(new SpewItem(e1, root, root, 1));
            // wait until the child is actually written, so writtenEmbeds > 0 and the only thing that
            // can suppress finalize is the cancellation guard (not the empty-container guard).
            for (int i = 0; i < 200 && coord.writtenCount() < 1; i++) {
                Thread.sleep(5);
            }
            assertThat(coord.writtenCount()).isEqualTo(1L);

            Thread.currentThread().interrupt(); // the parse thread is cancelled
            try {
                coord.spew(root);
            } finally {
                Thread.interrupted(); // clear so we don't poison the test-runner thread
            }
        }

        // A cancelled parse must not be finalized as a complete container (its count is unreliable and
        // the container was not fully processed); the child was still written.
        assertThat(spewer.written).contains("e1");
        assertThat(spewer.finalizedRoots).isEmpty();
    }

    @Test
    public void testDoesNotFinalizeRootWithoutChildren() throws Exception {
        RecordingSpewer spewer = new RecordingSpewer();
        TikaDocument root = doc("root", new StringReader("root-body"));

        try (StreamingSpewCoordinator coord = new StreamingSpewCoordinator(spewer, 16)) {
            coord.spew(root); // a plain, childless document is not a container: no finalize
        }

        assertThat(spewer.written).contains("root");
        assertThat(spewer.finalizedRoots).isEmpty();
    }

    @Test
    public void testWritesRootStubWhenParseAbortsBeforeRootIsWritten() throws Exception {
        RecordingSpewer spewer = new RecordingSpewer();
        TikaDocument root = doc("root", new StringReader("root-body"));
        TikaDocument e1 = doc("e1", new StringReader("a"));

        try (StreamingSpewCoordinator coord = new StreamingSpewCoordinator(spewer, 16)) {
            // Production starts the worker before the parse; simulate that so the embed drains while
            // the parse runs. The parse then throws (timeout/cancel) BEFORE the foreground wrote the
            // root: spew(root) is never called; close() runs via try-with-resources.
            coord.start();
            coord.promise();
            coord.ready(new SpewItem(e1, root, root, 1));
        }

        // The embed was written; the root would be orphaned, so close() must write a stub for it,
        // recording the number of children actually written (1) so the root can be marked partial.
        assertThat(spewer.written).contains("e1");
        assertThat(spewer.rootStubs).contains("root");
        assertThat(spewer.rootStubChildCounts).containsOnly(1L);
    }

    @Test
    public void testDoesNotWriteRootStubOnNormalCompletion() throws Exception {
        RecordingSpewer spewer = new RecordingSpewer();
        TikaDocument root = doc("root", new StringReader("root-body"));
        TikaDocument e1 = doc("e1", new StringReader("a"));

        try (StreamingSpewCoordinator coord = new StreamingSpewCoordinator(spewer, 16)) {
            coord.promise();
            coord.ready(new SpewItem(e1, root, root, 1));
            coord.spew(root); // foreground writes the real root
        }

        // The real root was written; no stub is needed.
        assertThat(spewer.written).contains("root", "e1");
        assertThat(spewer.rootStubs).isEmpty();
    }

    @Test
    public void testDoesNotWriteRootStubWhenNoEmbedsWereWritten() throws Exception {
        RecordingSpewer spewer = new RecordingSpewer();

        try (StreamingSpewCoordinator coord = new StreamingSpewCoordinator(spewer, 16)) {
            // Parse aborted before producing any embed: nothing to orphan, so no stub.
        }

        assertThat(spewer.rootStubs).isEmpty();
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

    @Test
    public void testIsolatesPerDocumentWriteFailureAndWritesRemainingEmbeds() throws Exception {
        RecordingSpewer spewer = new RecordingSpewer();
        spewer.failOnId = "e2"; // throw on e2, but succeed on e1 and e3
        TikaDocument root = doc("root", new StringReader("root-body"));
        TikaDocument e1 = doc("e1", new StringReader("a"));
        TikaDocument e2 = doc("e2", new StringReader("b"));
        TikaDocument e3 = doc("e3", new StringReader("c"));

        try (StreamingSpewCoordinator coord = new StreamingSpewCoordinator(spewer, 16)) {
            coord.promise();
            coord.ready(new SpewItem(e1, root, root, 1));
            coord.promise();
            coord.ready(new SpewItem(e2, root, root, 1));
            coord.promise();
            coord.ready(new SpewItem(e3, root, root, 1));

            try {
                coord.spew(root);
                org.junit.Assert.fail("expected a worker failure exception to propagate from spew");
            } catch (IOException expected) {
                assertThat(expected.getMessage()).contains("boom writing specific ID e2");
            }
        }

        // Under per-document isolation, e1 and e3 should still be successfully written,
        // and only e2 should have been skipped/thrown.
        assertThat(spewer.written).contains("root", "e1", "e3");
        assertThat(spewer.written).excludes("e2");
    }
}
