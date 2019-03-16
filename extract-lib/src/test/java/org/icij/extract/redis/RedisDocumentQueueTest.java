package org.icij.extract.redis;

import org.icij.extract.QueueFilterBuilder;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.PathIdentifier;
import org.icij.task.Options;
import org.junit.After;
import org.junit.Test;

import java.util.HashMap;
import java.util.stream.Stream;

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

        assertThat(queue.removeDuplicatePaths()).isEqualTo(1);

        assertThat(queue.size()).isEqualTo(2);
    }

    @Test
    public void testRemoveExtracted() throws Exception {
        queue.put(factory.create(get("/foo/bar")));
        queue.put(factory.create(get("/foo/baz")));

        RedisDocumentQueue filtered = (RedisDocumentQueue) new QueueFilterBuilder()
                .filter(queue)
                .with(() -> Stream.of(get("/foo/bar")))
                .execute();

        assertThat(filtered.getName()).isEqualTo(queue.getName() + ":filtered");
        assertThat(filtered.size()).isEqualTo(1);
        filtered.delete();
    }

    @After public void tearDown() { queue.delete();}
}
