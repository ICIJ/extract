package org.icij.extract.extractor;

import com.pff.PSTFile;
import com.pff.PstFolderPathResolver;
import org.icij.extract.document.EmbeddedTikaDocument;
import org.icij.extract.document.TikaDocument;
import org.icij.spewer.Spewer;
import org.icij.task.Options;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static org.fest.assertions.Assertions.assertThat;

/**
 * Sacred-invariant guard: a PST parsed with folder fan-out ON must produce the SAME SET of
 * {embed id, X-TIKA:digest:*, artifact filename(=id), name} as the serial walk. Fan-out changes
 * only the ORDER independent message-subtrees are processed in; no identity component crosses a
 * message-subtree boundary. OCR is disabled so the test does not depend on Tesseract.
 *
 * The test also asserts the EXACT embed count (not just the set) across multiple fan-out runs to
 * surface any concurrent mutation that would silently drop or duplicate an embed.
 */
public class PstFolderFanoutDeterminismTest {

    private static final String PST = "/documents/pst/testPST.pst";
    private static final int FANOUT_REPETITIONS = 5;

    private static final class ParseResult {
        final TreeSet<String> ids;
        final int count;
        ParseResult(TreeSet<String> ids, int count) {
            this.ids = ids;
            this.count = count;
        }
    }

    private ParseResult parseResult(boolean fanout) throws Exception {
        try (Extractor extractor = new Extractor(Options.from(Map.of(
                "pstFolderFanout", String.valueOf(fanout),
                "pstParseParallelism", "8",
                "ocr", "false",
                "progressHeartbeatInterval", "0")))) {
            TikaDocument doc = extractor.extract(
                    Paths.get(getClass().getResource(PST).toURI()));
            // Drive the parse to completion (PST emits all content as embeds).
            try (java.io.Reader r = doc.getReader()) { Spewer.toString(r); }
            TreeSet<String> ids = new TreeSet<>();
            int count = collectCount(doc, ids);
            return new ParseResult(ids, count);
        }
    }

    /**
     * Depth-first walk: collects identity strings into {@code ids} and returns the TOTAL number
     * of embedded documents (counting every node in the tree, not just unique ids).
     */
    private int collectCount(TikaDocument doc, TreeSet<String> ids) throws Exception {
        int count = 0;
        for (EmbeddedTikaDocument embed : doc.getEmbeds()) {
            ids.add(embed.getId() + "|"
                    + embed.getMetadata().get("X-TIKA:digest:SHA256") + "|"
                    + embed.getMetadata().get(org.apache.tika.metadata.TikaCoreProperties.RESOURCE_NAME_KEY));
            count++;
            count += collectCount(embed, ids);
        }
        return count;
    }

    @Test(timeout = 120_000)
    public void testFanoutProducesIdenticalIdentitySetAsSerial() throws Exception {
        // Guard: prove the fixture actually triggers the fan-out branch (canFanOut needs
        // folderPaths.size() > 1), so the serial==fanout equality below is a real
        // comparison and not a trivial serial-vs-serial pass.
        PSTFile pst = new PSTFile(
                Paths.get(getClass().getResource(PST).toURI()).toString());
        try {
            assertThat(PstFolderPathResolver.folderPaths(pst).size()).isGreaterThan(1);
        } finally {
            pst.getFileHandle().close();
        }

        ParseResult serial = parseResult(false);
        assertThat(serial.ids).isNotEmpty();
        assertThat(serial.count).isGreaterThan(0);

        for (int i = 0; i < FANOUT_REPETITIONS; i++) {
            ParseResult fanout = parseResult(true);
            assertThat(fanout.ids)
                    .as("fan-out identity set must equal serial (run " + (i + 1) + ")")
                    .isEqualTo(serial.ids);
            assertThat(fanout.count)
                    .as("fan-out embed count must equal serial (run " + (i + 1) + "): expected "
                            + serial.count + " but got " + fanout.count)
                    .isEqualTo(serial.count);
        }
    }
}
