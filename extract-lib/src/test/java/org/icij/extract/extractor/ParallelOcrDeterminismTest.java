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

    /**
     * Regression guard for the deferred-OCR spool-lifetime bug.
     *
     * Before the fix, the async OCR task read the embed bytes from tis's OWN transient spool, which
     * the container parser deletes the moment it advances to the next entry. The async read then hit
     * NoSuchFileException, which spawnEmbeddedDeferred records into the embed's Metadata under
     * TIKA_META_EXCEPTION_EMBEDDED_STREAM — silently losing the OCR text. The fix copies the bytes
     * into a parse-owned temp file (lifetime spans until the post-spew root-reader close), so the
     * async read succeeds and no exception is recorded.
     *
     * This drives a PARALLEL run (ocrParallelism=8, ocrFanout=true) over the same image fixture and
     * asserts NO embed carries the embedded-stream exception key after the OCR task has actually run.
     */
    @Test
    public void testParallelDeferredOcrReadsDoNotFail() throws Exception {
        try (Extractor extractor = new Extractor(Options.from(Map.of(
                "ocrParallelism", "8",
                "ocrFanout", "true",
                "progressHeartbeatInterval", "0")))) {

            TikaDocument doc = extractor.extract(
                    Paths.get(getClass().getResource(FIXTURE_RESOURCE).getPath()));

            // Drain the root reader: this drives the per-embed backstop so every deferred OCR task
            // actually runs and performs its Files.newInputStream(ocrInput) read before we assert.
            try (java.io.Reader r = doc.getReader()) {
                Spewer.toString(r);
            }

            List<TikaDocument> embeds = new ArrayList<>();
            collectEmbeds(doc, embeds);
            assertThat(embeds).isNotEmpty();

            // No embed may carry the embedded-stream exception key. Before the fix, the image embed's
            // async read fails with NoSuchFileException and this key is present; after the fix, absent.
            for (TikaDocument embed : embeds) {
                assertThat(embed.getMetadata()
                        .get(org.apache.tika.metadata.TikaCoreProperties.TIKA_META_EXCEPTION_EMBEDDED_STREAM))
                        .as("embed should have no recorded embedded-stream exception (OCR read must not fail)")
                        .isNull();
            }
        }
    }

    /** Depth-first walk of the embed tree; appends each embed document. */
    private void collectEmbeds(TikaDocument doc, List<TikaDocument> embeds) throws Exception {
        for (EmbeddedTikaDocument embed : doc.getEmbeds()) {
            embeds.add(embed);
            collectEmbeds(embed, embeds);
        }
    }
}
