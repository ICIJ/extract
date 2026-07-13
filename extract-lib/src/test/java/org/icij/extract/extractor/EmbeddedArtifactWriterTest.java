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
}
