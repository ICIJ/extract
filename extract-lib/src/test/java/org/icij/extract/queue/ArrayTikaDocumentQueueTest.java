package org.icij.extract.queue;

import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.PathIdentifier;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Path;

import static java.nio.file.Paths.get;
import static java.util.Arrays.stream;
import static org.fest.assertions.Assertions.assertThat;

public class ArrayTikaDocumentQueueTest {

    private final DocumentFactory factory = new DocumentFactory().withIdentifier(new PathIdentifier());

    @Test
    public void testCloseClearsTheQueue() {
        final DocumentQueue queue = new ArrayDocumentQueue(1);

        Assert.assertEquals(0, queue.size());
        queue.add(factory.create(get("essay.txt")));
        Assert.assertEquals(1, queue.size());

        queue.clear();
        Assert.assertEquals(0, queue.size());
    }

    @Test
    public void testRemoveDuplicates() throws Exception {
        final DocumentQueue queue = createQueue(get("/foo/bar"), get("/foo/baz"), get("/foo/bar"));

        queue.removeDuplicatePaths();

        assertThat(queue.size()).isEqualTo(2);
    }

    private DocumentQueue createQueue(Path... paths) {
        final DocumentQueue queue = new ArrayDocumentQueue(3);
        stream(paths).forEach(p -> queue.add(factory.create(p)));
        return queue;
    }
}
