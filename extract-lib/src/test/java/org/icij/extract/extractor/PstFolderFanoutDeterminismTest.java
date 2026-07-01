package org.icij.extract.extractor;

import org.apache.tika.metadata.TikaCoreProperties;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.EmbeddedTikaDocument;
import org.icij.extract.document.TikaDocument;
import org.icij.spewer.Spewer;
import org.icij.task.Options;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.Map;
import java.util.TreeSet;

import static org.fest.assertions.Assertions.assertThat;

/**
 * Sacred-invariant guard. A PST parsed with folder fan-out ON must produce the SAME SET and the
 * SAME COUNT of {embed id, X-TIKA:digest:SHA384, resource-name, PST_FOLDER_PATH, OCR_PARSER} as the
 * serial walk. Driven with the PRODUCTION DigestIdentifier (SHA-384): the embed id derives from
 * parent.getId(), so the fork-isolation bug (attachment attributed to root instead of its message)
 * changes the id and this test fails. (The previous version used the default path-based identifier,
 * which is why it missed the bug.) OCR is disabled so the test does not depend on Tesseract.
 */
public class PstFolderFanoutDeterminismTest {

    private static final String PST = "/documents/pst/testPST.pst";

    private DocumentFactory digestFactory() {
        // Mirror DatashareExtractIntegrationTest.createExtractor: SHA-384 digest id, resolvable
        // on-demand, so the id depends on parent.getId() + name (the fork-sensitive inputs).
        return new DocumentFactory().withIdentifier(
                new org.icij.extract.document.DigestIdentifier("SHA-384", java.nio.charset.StandardCharsets.UTF_8));
    }

    private TreeSet<String> identitySet(final boolean fanout) throws Exception {
        final DocumentFactory factory = digestFactory();
        try (Extractor extractor = new Extractor(factory, Options.from(Map.of(
                "pstFolderFanout", String.valueOf(fanout),
                "pstParseParallelism", "8",
                "ocr", "false",
                "digestAlgorithm", "SHA-384")))) {
            final TikaDocument doc = extractor.extract(Paths.get(getClass().getResource(PST).toURI()));
            try (java.io.Reader r = doc.getReader()) { Spewer.toString(r); }
            final TreeSet<String> ids = new TreeSet<>();
            collect(doc, ids);
            return ids;
        }
    }

    private void collect(final TikaDocument doc, final TreeSet<String> ids) throws Exception {
        for (final EmbeddedTikaDocument embed : doc.getEmbeds()) {
            ids.add(embed.getId() + "|"
                    + embed.getMetadata().get("X-TIKA:digest:SHA384") + "|"
                    + embed.getMetadata().get(TikaCoreProperties.RESOURCE_NAME_KEY) + "|"
                    + embed.getMetadata().get(org.apache.tika.metadata.PST.PST_FOLDER_PATH) + "|"
                    + embed.getMetadata().get(org.icij.extract.ocr.OCRParser.OCR_PARSER));
            collect(embed, ids);
        }
    }

    private int count(final TikaDocument doc) {
        int n = 0;
        for (final EmbeddedTikaDocument embed : doc.getEmbeds()) {
            n += 1 + count(embed);
        }
        return n;
    }

    @Test(timeout = 180_000)
    public void testFanoutIdentityMatchesSerialAcrossRepeatedRuns() throws Exception {
        final TreeSet<String> serial = identitySet(false);
        assertThat(serial).isNotEmpty();
        // testPST.pst must be genuinely multi-folder so fan-out actually runs (see Task 5 in the
        // original plan): its 5 folders drive walkFoldersParallel.
        for (int run = 0; run < 5; run++) {
            final TreeSet<String> fanout = identitySet(true);
            assertThat(fanout).as("identity set, run " + run).isEqualTo(serial);
        }
    }

    @Test(timeout = 180_000)
    public void testFanoutEmbedCountMatchesSerial() throws Exception {
        final DocumentFactory factory = digestFactory();
        int serialCount;
        try (Extractor extractor = new Extractor(factory, Options.from(Map.of(
                "pstFolderFanout", "false", "ocr", "false", "digestAlgorithm", "SHA-384")))) {
            final TikaDocument doc = extractor.extract(Paths.get(getClass().getResource(PST).toURI()));
            try (java.io.Reader r = doc.getReader()) { Spewer.toString(r); }
            serialCount = count(doc);
        }
        assertThat(serialCount).isGreaterThan(1);
        try (Extractor extractor = new Extractor(digestFactory(), Options.from(Map.of(
                "pstFolderFanout", "true", "pstParseParallelism", "8", "ocr", "false",
                "digestAlgorithm", "SHA-384")))) {
            final TikaDocument doc = extractor.extract(Paths.get(getClass().getResource(PST).toURI()));
            try (java.io.Reader r = doc.getReader()) { Spewer.toString(r); }
            assertThat(count(doc)).isEqualTo(serialCount);
        }
    }
}
