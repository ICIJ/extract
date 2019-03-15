package org.icij.extract;

import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.PathIdentifier;
import org.icij.extract.queue.ArrayDocumentQueue;
import org.icij.extract.queue.DocumentQueue;
import org.junit.Test;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static java.nio.file.Paths.get;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.fest.assertions.Assertions.assertThat;

public class QueueFilterBuilderTest {
    private final DocumentFactory factory = new DocumentFactory().withIdentifier(new PathIdentifier());

    @Test
    public void test_empty_filter_returns_same_queue() {
        ExtractedStreamer streamer = createStreamer(new ArrayList<>());
        DocumentQueue tikaDocuments = createDocumentQueue(asList(get("/foo/bar"), get("/baz/qux")));

        DocumentQueue result = new QueueFilterBuilder().filter(tikaDocuments).with(streamer).execute();

        assertThat(result.size()).isEqualTo(2);
    }

    @Test
    public void test_filter_queue() {
        ExtractedStreamer streamer = createStreamer(asList(get("/foo/bar"), get("/baz/qux")));
        DocumentQueue tikaDocuments = createDocumentQueue(asList(get("/foo/bar"), get("/baz/qux"), get("/foo/liz")));

        DocumentQueue result = new QueueFilterBuilder().filter(tikaDocuments).with(streamer).execute();

        assertThat(result.size()).isEqualTo(1);
        assertThat(result.poll()).isEqualTo(factory.create(get("/foo/liz")));
    }

    private ExtractedStreamer createStreamer(List<Path> pathList) {
        return new ExtractedStreamer() {
            @Override
            public Stream<Path> extractedDocuments() {
                return pathList.stream();
            }
        };
    }

    private DocumentQueue createDocumentQueue(List<Path> pathList) {
        ArrayDocumentQueue tikaDocuments = new ArrayDocumentQueue(pathList.size());
        tikaDocuments.addAll(pathList.stream().map(factory::create).collect(toList()));
        return tikaDocuments;
    }
}
