package org.icij.extract;

import org.icij.extract.queue.ArrayDocumentQueue;
import org.icij.extract.queue.DocumentQueue;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;

import static java.nio.file.Paths.get;
import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static org.fest.assertions.Assertions.assertThat;

public class QueueFilterBuilderTest {
    @Test
    public void test_filter_zero_results() throws IOException {
        ExtractedStreamer streamer = createStreamer();
        DocumentQueue tikaDocuments = createDocumentQueue(get("/foo/bar"), get("/baz/qux"));

        assertThat(new QueueFilterBuilder().filter(tikaDocuments).with(streamer).execute()).isEqualTo(0);
    }

    @Test
    public void test_filter_queue() throws IOException {
        ExtractedStreamer streamer = createStreamer(get("/foo/bar"), get("/baz/qux"));
        DocumentQueue tikaDocuments = createDocumentQueue(get("/foo/bar"), get("/baz/qux"), get("/foo/liz"));

        assertThat(new QueueFilterBuilder().filter(tikaDocuments).with(streamer).execute()).isEqualTo(2);

        assertThat(Objects.requireNonNull(tikaDocuments.poll()).toString()).isEqualTo("/foo/liz");
    }

    private ExtractedStreamer createStreamer(Path... pathList) {
        return () -> stream(pathList);
    }

    private DocumentQueue createDocumentQueue(Path... pathList) {
        ArrayDocumentQueue tikaDocuments = new ArrayDocumentQueue(pathList.length);
        tikaDocuments.addAll(asList(pathList));
        return tikaDocuments;
    }
}
