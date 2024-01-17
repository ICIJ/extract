package org.icij.extract.redis;

import org.icij.task.Options;
import org.junit.After;
import org.junit.Test;

import java.nio.file.Path;
import java.util.HashMap;

import static java.nio.file.Paths.get;
import static org.fest.assertions.Assertions.assertThat;

public class RedisDocumentQueueTest {
    private final RedisDocumentQueue<Path> pathQueue = new RedisDocumentQueue<>(Options.from(new HashMap<>() {{
        put("redisAddress", "redis://redis:6379");
        put("queueName", "test:path:queue");
    }}), Path.class);
    private final RedisDocumentQueue<String> stringQueue = new RedisDocumentQueue<>(Options.from(new HashMap<>() {{
        put("redisAddress", "redis://redis:6379");
        put("queueName", "test:string:queue");
    }}), String.class);

    @Test
    public void testRemoveDuplicates() throws Exception {
        pathQueue.put(get("/foo/bar"));
        pathQueue.put(get("/foo/baz"));
        pathQueue.put(get("/foo/bar"));
        pathQueue.put(get("/foo/bar"));

        assertThat(pathQueue.removeDuplicates()).isEqualTo(2);

        assertThat(pathQueue.size()).isEqualTo(2);
    }

    @Test
    public void testDelete() throws Exception {
        pathQueue.put(get("/foo/bar"));
        pathQueue.put(get("/foo/baz"));
        pathQueue.put(get("/foo/bar"));
        pathQueue.put(get("/foo/bar"));
        assertThat(pathQueue.size()).isEqualTo(4);
        pathQueue.delete();
        assertThat(pathQueue.size()).isEqualTo(0);
    }

    @Test
    public void testStringQueue() throws Exception {
        stringQueue.put("foo");
        assertThat(stringQueue.take()).isEqualTo("foo");
        assertThat(stringQueue.size()).isEqualTo(0);
    }

    @After public void tearDown() {
        pathQueue.delete();
        stringQueue.delete();
    }
}
