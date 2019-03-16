package org.icij.extract.redis;

import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.PathIdentifier;
import org.icij.task.Options;
import org.junit.After;
import org.junit.Test;

import java.util.HashMap;

import static java.nio.file.Paths.get;
import static org.fest.assertions.Assertions.assertThat;

public class RedisDocumentQueueTest {
    private DocumentFactory factory = new DocumentFactory().withIdentifier(new PathIdentifier());;
    private RedisDocumentQueue queue = new RedisDocumentQueue(factory, Options.from(new HashMap<String, String>() {{
        put("redisAddress", "redis://redis:6379");
        put("queueName", "test:queue");
    }}));

    @Test
    public void testRemoveDuplicates() throws Exception {
        queue.put(factory.create(get("/foo/bar")));
        queue.put(factory.create(get("/foo/baz")));
        queue.put(factory.create(get("/foo/bar")));

        queue.removeDuplicatePaths();

        assertThat(queue.size()).isEqualTo(2);
    }

    @After public void tearDown() { queue.delete();}
}
