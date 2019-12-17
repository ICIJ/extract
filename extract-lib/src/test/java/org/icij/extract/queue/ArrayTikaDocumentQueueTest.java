package org.icij.extract.queue;

import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Path;

import static java.nio.file.Paths.get;
import static java.util.Arrays.asList;
import static org.fest.assertions.Assertions.assertThat;

public class ArrayTikaDocumentQueueTest {
    @Test
    public void testCloseClearsTheQueue() {
        final DocumentQueue queue = new ArrayDocumentQueue(1);

        Assert.assertEquals(0, queue.size());
        queue.add(get("essay.txt"));
        Assert.assertEquals(1, queue.size());

        queue.clear();
        Assert.assertEquals(0, queue.size());
    }

    @Test
    public void testRemoveDuplicates() {
        final DocumentQueue queue = createQueue(get("/foo/bar"), get("/foo/baz"), get("/foo/bar"));

        assertThat(queue.removeDuplicates()).isEqualTo(1);

        assertThat(queue.size()).isEqualTo(2);
    }

    @Test
    public void testRemoveNValues() {
        final DocumentQueue queue = createQueue(get("/foo/bar"), get("/foo/bar"), get("/foo/bar"));

        assertThat(queue.remove(get("/foo/bar"), 2)).isTrue();

        assertThat(queue.size()).isEqualTo(1);
    }

    @Test
    public void testRemoveNValuesNoValuesRemoved() {
        final DocumentQueue queue = createQueue();
        assertThat(queue.remove(get("/foo/bar"), 0)).isFalse();
    }

    @Test
    public void testRemoveAllValues() {
        final DocumentQueue queue = createQueue(get("/foo/baz"), get("/foo/bar"), get("/foo/bar"), get("/foo/bar"));

        assertThat(queue.remove(get("/foo/bar"), 0)).isTrue();

        assertThat(queue.size()).isEqualTo(1);
    }

    private DocumentQueue createQueue(Path... paths) {
        final DocumentQueue queue = new ArrayDocumentQueue(4);
        queue.addAll(asList(paths));
        return queue;
    }
}
