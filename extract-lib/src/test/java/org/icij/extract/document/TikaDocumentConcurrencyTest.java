package org.icij.extract.document;

import org.apache.tika.metadata.Metadata;
import org.junit.Test;

import static org.apache.tika.metadata.TikaCoreProperties.RESOURCE_NAME_KEY;

import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static org.fest.assertions.Assertions.assertThat;

public class TikaDocumentConcurrencyTest {

    private TikaDocument root() {
        final String algorithm = "SHA-384";
        final Metadata metadata = new Metadata();
        // Seed the file-digest hash so the DigestIdentifier can generate a root id (a bare document with no
        // digest in its metadata has a null hash, which is unrelated to the thread-safety under test here).
        metadata.set(Identifier.getKey(algorithm), "ROOTHASH");
        metadata.set(RESOURCE_NAME_KEY, "root");
        return new DocumentFactory()
                .withIdentifier(new DigestIdentifier(algorithm, StandardCharsets.UTF_8))
                .create(Paths.get("/tmp/root"), metadata);
    }

    @Test(timeout = 30_000)
    public void testGetEmbedsSnapshotDoesNotThrowWhileAdding() throws Exception {
        final TikaDocument root = root();
        final ExecutorService pool = Executors.newFixedThreadPool(2);
        final CountDownLatch go = new CountDownLatch(1);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final Runnable adder = () -> { try { go.await(); for (int i = 0; i < 5000; i++) root.addEmbed(new Metadata()); }
                catch (final Throwable t) { failure.set(t); } };
        final Runnable reader = () -> { try { go.await(); for (int i = 0; i < 5000; i++) { for (EmbeddedTikaDocument e : root.getEmbeds()) { e.hashCode(); } } }
                catch (final Throwable t) { failure.set(t); } };
        pool.submit(adder); pool.submit(reader);
        go.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(25, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        assertThat(failure.get()).isNull(); // no ConcurrentModificationException
    }

    @Test(timeout = 30_000)
    public void testGetIdIsStableUnderConcurrentAccess() throws Exception {
        final TikaDocument root = root();
        final ExecutorService pool = Executors.newFixedThreadPool(8);
        final CountDownLatch go = new CountDownLatch(1);
        final List<java.util.concurrent.Future<String>> futures = new java.util.ArrayList<>();
        for (int i = 0; i < 8; i++) {
            futures.add(pool.submit(() -> { go.await(); return root.getId(); }));
        }
        go.countDown();
        final String first = futures.get(0).get();
        for (final java.util.concurrent.Future<String> f : futures) {
            assertThat(f.get()).isEqualTo(first);
        }
        pool.shutdownNow();
    }
}
