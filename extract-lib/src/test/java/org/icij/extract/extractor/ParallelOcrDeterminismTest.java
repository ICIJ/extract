package org.icij.extract.extractor;

import org.icij.extract.document.EmbeddedTikaDocument;
import org.icij.extract.document.TikaDocument;
import org.icij.spewer.Spewer;
import org.icij.task.Options;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.fest.assertions.Assertions.assertThat;

/**
 * Regression guard: embed IDs must be byte-identical whether OCR runs serially
 * (ocrParallelism=1, ocrFanout=false) or in a parallel pool (ocrParallelism=8, ocrFanout=true).
 *
 * IDs derive from the eager content digest computed synchronously in EmbedSpawner.parseEmbedded(),
 * NOT from any OCR text produced later. Therefore this test must pass even when Tesseract/OCR is
 * absent from the environment.
 *
 * Fixture: image_attachment.eml
 *   A minimal RFC 2822 email with a 1x1 PNG image as a non-inline attachment. Tika parses this
 *   as a single embed with Content-Type "image/ocr-png", which satisfies EmbedSpawner.isOcrEligible
 *   (the type starts with "image/"). When ocrFanout=true, the embed is handled by
 *   spawnEmbeddedDeferred() — the deferred OCR path — so this fixture directly exercises the
 *   digest-synchronous code path that the invariant guards.
 *
 * Materialization: the embed tree is populated lazily as the root document's Reader is consumed.
 * We therefore drain the root reader to completion before walking getEmbeds(), ensuring every
 * embed's digest (and thus its ID) has been computed.
 */
public class ParallelOcrDeterminismTest {

    // Fixture that produces a non-empty embed list including an image/ocr-png embed that
    // exercises the deferred OCR code path in EmbedSpawner.spawnEmbeddedDeferred().
    private static final String FIXTURE_RESOURCE = "/documents/image_attachment.eml";

    /**
     * Extract the fixture with the given OCR settings, drain the root reader to force all embedded
     * parse work to complete, then collect embed IDs depth-first.
     */
    private List<String> idsAfterExtract(int parallelism, boolean fanout) throws Exception {
        try (Extractor extractor = new Extractor(Options.from(Map.of(
                "ocrParallelism", String.valueOf(parallelism),
                "ocrFanout", String.valueOf(fanout),
                "progressHeartbeatInterval", "0")))) {

            TikaDocument doc = extractor.extract(
                    Paths.get(getClass().getResource(FIXTURE_RESOURCE).getPath()));

            // Drain the root reader to completion.  This drives all deferred OCR tasks and ensures
            // each embed's digest is written into its Metadata before we call getId() below.
            try (java.io.Reader r = doc.getReader()) {
                Spewer.toString(r);
            }

            List<String> ids = new ArrayList<>();
            collectIds(doc, ids);
            return ids;
        }
    }

    /** Depth-first walk of the embed tree; appends each embed's ID. */
    private void collectIds(TikaDocument doc, List<String> ids) throws Exception {
        for (EmbeddedTikaDocument embed : doc.getEmbeds()) {
            ids.add(embed.getId());
            collectIds(embed, ids);
        }
    }

    @Test
    public void testParallelAndSerialProduceIdenticalEmbedIds() throws Exception {
        List<String> serial   = idsAfterExtract(1, false);
        List<String> parallel = idsAfterExtract(8, true);

        // The embed list must not be empty — verifies the fixture actually has embeds.
        assertThat(serial).isNotEmpty();

        // Core invariant: parallel OCR must not change embed IDs.
        assertThat(parallel).isEqualTo(serial);
    }
}
