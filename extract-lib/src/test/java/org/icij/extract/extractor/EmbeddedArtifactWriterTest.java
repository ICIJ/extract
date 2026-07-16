package org.icij.extract.extractor;

import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.icij.spewer.MetadataTransformer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.fest.assertions.Assertions.assertThat;

public class EmbeddedArtifactWriterTest {

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    private static final String ID = "abcd1234ef567890abcd1234ef567890abcd1234ef567890abcd1234ef567890";

    private Metadata metadataWithName(String name) {
        Metadata metadata = new Metadata();
        metadata.set(TikaCoreProperties.RESOURCE_NAME_KEY, name);
        return metadata;
    }

    @Test
    public void test_rawPath_is_content_addressed_with_raw_leaf() {
        Path root = tmp.getRoot().toPath();
        assertThat(EmbeddedArtifactWriter.rawPath(root, ID).toFile())
                .isEqualTo(root.resolve("ab").resolve("cd").resolve(ID).resolve("raw").toFile());
    }

    @Test
    public void test_write_from_path_creates_raw_and_sidecar() throws Exception {
        Path root = tmp.getRoot().toPath();
        Path source = tmp.newFile("payload.bin").toPath();
        Files.write(source, "hello".getBytes());
        Metadata metadata = metadataWithName("payload.bin");

        File written = EmbeddedArtifactWriter.write(root, ID, metadata, source);

        assertThat(written).isEqualTo(EmbeddedArtifactWriter.rawPath(root, ID).toFile());
        assertThat(written).isFile();
        assertThat(Files.readAllBytes(written.toPath())).isEqualTo("hello".getBytes());
        File sidecar = new File(written + ".json");
        assertThat(sidecar).isFile();
        assertThat(Files.readAllBytes(sidecar.toPath()))
                .isEqualTo(new MetadataTransformer(metadata).transform().getBytes(Charset.defaultCharset()));
    }

    @Test
    public void test_write_from_tika_input_stream_creates_raw_and_sidecar_and_resets() throws Exception {
        Path root = tmp.getRoot().toPath();
        Path source = tmp.newFile("stream.bin").toPath();
        Files.write(source, "streamed".getBytes());
        Metadata metadata = metadataWithName("stream.bin");

        try (TikaInputStream tis = TikaInputStream.get(source)) {
            File written = EmbeddedArtifactWriter.write(root, ID, metadata, tis);
            assertThat(Files.readAllBytes(written.toPath())).isEqualTo("streamed".getBytes());
            assertThat(new File(written + ".json")).isFile();
            // reset() was called: the stream is readable again from the start.
            assertThat(tis.read()).isEqualTo((int) 's');
        }
    }

    @Test
    public void test_write_overwrites_existing_payload() throws Exception {
        Path root = tmp.getRoot().toPath();
        Path first = tmp.newFile("first.bin").toPath();
        Files.write(first, "one".getBytes());
        Path second = tmp.newFile("second.bin").toPath();
        Files.write(second, "twotwo".getBytes());
        Metadata metadata = metadataWithName("x");

        EmbeddedArtifactWriter.write(root, ID, metadata, first);
        File written = EmbeddedArtifactWriter.write(root, ID, metadata, second);

        assertThat(Files.readAllBytes(written.toPath())).isEqualTo("twotwo".getBytes());
    }

    // F1/N2: two ARTIFACT workers can re-extract the same root concurrently under
    // parallelism > 1 and both call write() for the SAME id at the same time. Before the fix,
    // write() copies straight to the final "raw" path (FileOutputStream/Files.copy with
    // REPLACE_EXISTING truncating in place), so a concurrent reader can observe a raw file whose
    // length is neither 0 nor the full expected size -- a truncation window that, combined with a
    // crash mid-write, is what leaves a permanently truncated cache entry (N2). After the fix
    // (write-to-temp-then-atomic-move), the final path only ever shows up complete or not at all.
    @Test(timeout = 60000)
    public void test_concurrent_writes_to_same_id_never_expose_a_partial_raw_file() throws Exception {
        Path root = tmp.getRoot().toPath();
        byte[] content = new byte[5 * 1024 * 1024];
        new Random(42).nextBytes(content);
        Path source = tmp.newFile("payload.bin").toPath();
        Files.write(source, content);
        Metadata metadata = metadataWithName("payload.bin");

        File rawFile = EmbeddedArtifactWriter.rawPath(root, ID).toFile();

        int writerThreads = 8;
        int iterations = 15;
        ExecutorService pool = Executors.newFixedThreadPool(writerThreads + 1);
        List<Throwable> errors = Collections.synchronizedList(new ArrayList<>());
        AtomicBoolean stop = new AtomicBoolean(false);
        AtomicInteger partialSightings = new AtomicInteger(0);

        Future<?> readerFuture = pool.submit(() -> {
            while (!stop.get()) {
                long len = rawFile.length(); // 0 when absent
                if (len != 0 && len != content.length) {
                    partialSightings.incrementAndGet();
                }
            }
        });

        List<Future<?>> writers = new ArrayList<>();
        for (int t = 0; t < writerThreads; t++) {
            writers.add(pool.submit(() -> {
                try {
                    for (int i = 0; i < iterations; i++) {
                        EmbeddedArtifactWriter.write(root, ID, metadata, source);
                    }
                } catch (Throwable e) {
                    errors.add(e);
                }
            }));
        }
        for (Future<?> f : writers) {
            f.get();
        }
        stop.set(true);
        readerFuture.get();
        pool.shutdown();
        pool.awaitTermination(5, TimeUnit.SECONDS);

        assertThat(errors).isEmpty();
        assertThat(partialSightings.get()).isEqualTo(0);
        assertThat(Files.readAllBytes(rawFile.toPath())).isEqualTo(content);
        assertThat(new File(rawFile + ".json")).isFile();
    }
}
