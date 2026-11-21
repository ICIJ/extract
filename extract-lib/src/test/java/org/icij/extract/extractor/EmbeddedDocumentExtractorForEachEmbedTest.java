package org.icij.extract.extractor;

import org.icij.extract.document.DigestIdentifier;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.EmbeddedTikaDocument;
import org.icij.extract.document.TikaDocument;
import org.icij.spewer.Spewer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.OutputStream;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.fest.assertions.Assertions.assertThat;

public class EmbeddedDocumentExtractorForEachEmbedTest {
    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    @Test
    public void test_visits_every_embed_once_with_index_time_ids_and_readable_bytes() throws Exception {
        Path zip = tmp.newFile("images.zip").toPath();
        writeZip(zip, "a.png", "b.png");
        DocumentFactory factory = new DocumentFactory().withIdentifier(new DigestIdentifier("SHA-256", UTF_8));

        List<String> expected = new ArrayList<>();
        try (Extractor extractor = new Extractor(factory)) {
            extractor.setDigester(new UpdatableDigester("prj", "SHA-256"));
            TikaDocument indexRoot = extractor.extract(zip);
            try (Reader reader = indexRoot.getReader()) {
                Spewer.toString(reader);
            }
            for (EmbeddedTikaDocument embed : indexRoot.getEmbeds()) {
                expected.add(embed.getId());
            }
        }

        List<String> visited = new ArrayList<>();
        List<Long> sizes = new ArrayList<>();
        TikaDocument freshRoot = factory.create(zip);
        EmbeddedDocumentExtractor embedExtractor = new EmbeddedDocumentExtractor(new UpdatableDigester("prj", "SHA-256"));
        embedExtractor.forEachEmbed(freshRoot, (id, bytes) -> {
            visited.add(id);
            sizes.add(bytes.toFile().length());
        });

        assertThat(expected).hasSize(2);
        assertThat(visited).hasSize(2);
        assertThat(visited).containsOnly(expected.toArray());
        assertThat(sizes).excludes(0L);
    }

    private static void writeZip(Path zip, String... names) throws Exception {
        try (OutputStream out = Files.newOutputStream(zip); ZipOutputStream zipOut = new ZipOutputStream(out)) {
            for (String name : names) {
                zipOut.putNextEntry(new ZipEntry(name));
                byte[] bytes = ("bytes of " + name).getBytes(UTF_8);
                zipOut.write(bytes);
                zipOut.closeEntry();
            }
        }
    }
}
