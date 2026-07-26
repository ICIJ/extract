package org.icij.extract.extractor;

import org.icij.extract.document.DigestIdentifier;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.TikaDocument;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.fest.assertions.Assertions.assertThat;

/**
 * The ARTIFACT re-extraction walk must cache exactly what the index accepted: every embed nested no
 * deeper than EmbedSpawner.DEFAULT_MAX_EMBED_DEPTH, and nothing deeper. Tika's SecureContentHandler
 * depth counters cannot express that bound here (they count the whole root parse and are corrupted by
 * any partial embed failure), so the walk carries its own guard, reusing the index's constant and
 * predicate. This pins both ends of the bound at once, including the off-by-one at the check site.
 */
public class EmbeddedDocumentExtractorEmbedDepthTest {

    @Rule public TemporaryFolder tmp = new TemporaryFolder();

    @Test
    public void cachesEveryLevelUpToTheIndexDepthBoundAndRefusesTheRest() throws Exception {
        final int bound = EmbedSpawner.DEFAULT_MAX_EMBED_DEPTH; // 20
        final int depth = bound + 5;

        // Levels 1..bound are kept, everything below is refused, so retrieval caches no more than the
        // index produced. An off-by-one at the check site shows up here as bound-1 or bound+1. This
        // also guards the reason the zip-bomb depth relaxation exists: without it, Tika's default
        // 10-level package-entry counter aborts this walk around level 9 (one <div
        // class="package-entry"> per embed level, the 10th throws), so the count lands nowhere near
        // the bound the index actually applies.
        assertThat(cachedRawCount(nestedZip(depth))).isEqualTo(bound);
    }

    // Runs the full ARTIFACT walk over `zipBytes` and counts the raw payloads it cached. The nested
    // zip yields exactly one embed per level, so the count IS the deepest level reached.
    private int cachedRawCount(final byte[] zipBytes) throws Exception {
        final Path zip = tmp.newFile().toPath();
        Files.write(zip, zipBytes);
        final Path artifacts = tmp.newFolder().toPath();

        final EmbeddedDocumentExtractor extractor =
                new EmbeddedDocumentExtractor(new UpdatableDigester("prj", "SHA-384"), artifacts);
        final TikaDocument document = new DocumentFactory()
                .withIdentifier(new DigestIdentifier("SHA-384", StandardCharsets.UTF_8))
                .create(zip);

        extractor.extractAll(document);

        try (var paths = Files.walk(artifacts)) {
            return (int) paths.filter(p -> p.getFileName().toString().equals("raw")).count();
        }
    }

    // Builds `depth` zips nested one inside the next, the innermost wrapping a small text leaf. The
    // outermost zip is the root document, so its entries are the embeds at levels 1..depth.
    private static byte[] nestedZip(final int depth) throws Exception {
        byte[] current = "needle".getBytes(StandardCharsets.UTF_8);
        String name = "leaf.txt";
        for (int level = 0; level < depth; level++) {
            final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (ZipOutputStream zip = new ZipOutputStream(bytes)) {
                zip.putNextEntry(new ZipEntry(name));
                zip.write(current);
                zip.closeEntry();
            }
            current = bytes.toByteArray();
            name = "level" + level + ".zip";
        }
        return current;
    }
}
