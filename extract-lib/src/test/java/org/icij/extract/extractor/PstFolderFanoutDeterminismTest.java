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
import java.util.TreeSet;

import static org.fest.assertions.Assertions.assertThat;

/**
 * Sacred-invariant guard: a PST parsed with folder fan-out ON must produce the SAME SET of
 * {embed id, X-TIKA:digest:*, artifact filename(=id), name} as the serial walk. Fan-out changes
 * only the ORDER independent message-subtrees are processed in; no identity component crosses a
 * message-subtree boundary. OCR is disabled so the test does not depend on Tesseract.
 */
public class PstFolderFanoutDeterminismTest {

    private static final String PST = "/documents/pst/testPST.pst";

    private TreeSet<String> identitySet(boolean fanout) throws Exception {
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
            collect(doc, ids);
            return ids;
        }
    }

    private void collect(TikaDocument doc, TreeSet<String> ids) throws Exception {
        for (EmbeddedTikaDocument embed : doc.getEmbeds()) {
            ids.add(embed.getId() + "|"
                    + embed.getMetadata().get("X-TIKA:digest:SHA256") + "|"
                    + embed.getMetadata().get(org.apache.tika.metadata.TikaCoreProperties.RESOURCE_NAME_KEY));
            collect(embed, ids);
        }
    }

    @Test(timeout = 120_000)
    public void testFanoutProducesIdenticalIdentitySetAsSerial() throws Exception {
        TreeSet<String> serial = identitySet(false);
        TreeSet<String> fanout = identitySet(true);
        assertThat(serial).isNotEmpty();
        assertThat(fanout).isEqualTo(serial);
    }
}
