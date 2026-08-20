package org.icij.extract.redis;

import org.icij.task.Options;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.redisson.RedissonShutdownException;
import org.redisson.api.RedissonClient;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.stream.Stream;

import static java.nio.file.Paths.get;
import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assume.assumeTrue;

public class RedisDocumentQueueTest {
    @Rule public TemporaryFolder tmp = new TemporaryFolder();

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
    public void test_path_with_undecodable_name_is_lost_by_the_queue() throws Exception {
        Path scanned = scannedPathOfFileWithUndecodableName();
        assertThat(new String(Files.readAllBytes(scanned))).isEqualTo("x");

        pathQueue.put(scanned);
        Path dequeued = pathQueue.take();

        // Same printed name, different bytes, so the loss leaves no trace in the logs.
        assertThat(dequeued.toString()).isEqualTo(scanned.toString());
        assertThat(dequeued.equals(scanned)).isFalse();
        assertThrows(NoSuchFileException.class, () -> Files.readAllBytes(dequeued));
    }

    @Test
    public void test_path_with_decodable_name_survives_the_queue() throws Exception {
        Path scanned = tmp.newFile("caf\u00e9.txt").toPath();

        pathQueue.put(scanned);
        Path dequeued = pathQueue.take();

        assertThat(dequeued.equals(scanned)).isTrue();
        assertThat(Files.exists(dequeued)).isTrue();
    }

    private Path scannedPathOfFileWithUndecodableName() throws Exception {
        // Paths.get() encodes the name with the platform charset and replaces whatever it cannot
        // encode, so a raw 0xFF byte in a file name can only be written by the shell.
        Process mkfile = new ProcessBuilder("sh", "-c", "printf x > \"$(printf 'bad\\377name.txt')\"")
                .directory(tmp.getRoot()).start();
        assumeTrue("file system refused a name that is not valid UTF-8", mkfile.waitFor() == 0);

        try (Stream<Path> files = Files.list(tmp.getRoot().toPath())) {
            Path scanned = files.findFirst().orElseThrow();
            assumeTrue("file name decoded without loss, nothing to test", scanned.toString().contains("\ufffd"));
            return scanned;
        }
    }

    @Test
    public void testStringQueue() throws Exception {
        stringQueue.put("foo");
        assertThat(stringQueue.take()).isEqualTo("foo");
        assertThat(stringQueue.size()).isEqualTo(0);
    }

    @Test(expected = RedissonShutdownException.class)
    public void test_close_should_shutdown_redis_if_created() throws IOException {
        RedisDocumentQueue<String> stringQueue = new RedisDocumentQueue<>(Options.from(new HashMap<>() {{
            put("redisAddress", "redis://redis:6379");
            put("queueName", "test:string:queue");
        }}), String.class);
        stringQueue.close();
        stringQueue.offer("foo");
    }

    @Test
    public void test_close_should_not_shutdown_redis_if_not_created() throws IOException {
        RedissonClient redissonClient = new RedissonClientFactory().withOptions(Options.from(new HashMap<>() {{
            put("redisAddress", "redis://redis:6379");
        }})).create();
        try (RedisDocumentQueue<String> ignored = new RedisDocumentQueue<>(redissonClient, "test:report", Charset.defaultCharset(), String.class)) {}
        assertThat(redissonClient.isShutdown()).isFalse();
        redissonClient.shutdown();
    }


    @After public void tearDown() {
        pathQueue.delete();
        stringQueue.delete();
    }
}
