package org.icij.extract.redis;

import org.icij.task.Options;
import org.junit.After;
import org.junit.Test;

import java.util.HashMap;

import static java.nio.file.Paths.get;
import static org.fest.assertions.Assertions.assertThat;

public class RedisDocumentQueueTest {
    private RedisDocumentQueue queue = new RedisDocumentQueue(Options.from(new HashMap<String, String>() {{
        put("redisAddress", "redis://redis:6379");
        put("queueName", "test:queue");
    }}));

    @Test
    public void testRemoveDuplicates() throws Exception {
        queue.put(get("/foo/bar"));
        queue.put(get("/foo/baz"));
        queue.put(get("/foo/bar"));
        queue.put(get("/foo/bar"));

        assertThat(queue.removeDuplicates()).isEqualTo(2);

        assertThat(queue.size()).isEqualTo(2);
    }

    @After public void tearDown() { queue.delete();}
}
