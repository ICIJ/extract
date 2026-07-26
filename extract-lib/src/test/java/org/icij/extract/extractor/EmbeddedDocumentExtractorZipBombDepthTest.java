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
 * The ARTIFACT re-extraction ({@link EmbeddedDocumentExtractor#extractAll}) re-parses documents the
 * index already accepted. Tika's default SecureContentHandler aborts a container nested past 100
 * levels (its zip-bomb guard) by throwing a SecureSAXException -- not a TikaException -- which the
 * embed catch misses, so every entry below the limit is silently dropped from the cache even though
 * the index kept them. This guards that the re-parse reaches entries nested well past that default.
 */
public class EmbeddedDocumentExtractorZipBombDepthTest {

    @Rule public TemporaryFolder tmp = new TemporaryFolder();

    private static final int DEPTH = 130; // > Tika's default 100-level package-entry-depth guard

    @Test
    public void extractsEntriesNestedPastTheDefaultZipBombDepth() throws Exception {
        Path zip = tmp.newFile("deep.zip").toPath();
        Files.write(zip, nestedZip(DEPTH));
        Path artifacts = tmp.newFolder("artifacts").toPath();

        EmbeddedDocumentExtractor extractor =
                new EmbeddedDocumentExtractor(new UpdatableDigester("prj", "SHA-384"), artifacts);
        TikaDocument document = new DocumentFactory()
                .withIdentifier(new DigestIdentifier("SHA-384", StandardCharsets.UTF_8))
                .create(zip);

        extractor.extractAll(document);

        // One raw artifact per nested container level reached. With the default guard the walk aborts
        // around level 100; with the guard relaxed it reaches every level.
        long rawFiles = Files.walk(artifacts).filter(p -> p.getFileName().toString().equals("raw")).count();
        assertThat(rawFiles).isGreaterThanOrEqualTo(DEPTH);
    }

    // Builds `depth` zips nested one inside the next, the innermost wrapping a small text leaf.
    private static byte[] nestedZip(final int depth) throws Exception {
        byte[] current = "needle".getBytes(StandardCharsets.UTF_8);
        String name = "leaf.txt";
        for (int level = 0; level < depth; level++) {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
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
