package org.icij.extract.extractor;

import org.icij.extract.document.DigestIdentifier;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.EmbeddedTikaDocument;
import org.icij.extract.document.TikaDocument;
import org.icij.spewer.Spewer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

import static org.fest.assertions.Assertions.assertThat;

/**
 * OST-2 regression guard. The ARTIFACT/download retrieval walk
 * ({@link EmbeddedDocumentExtractor#extractAll}) must write each embed's raw bytes under the SAME
 * content-addressed id the INDEX walk ({@link EmbedSpawner}, via {@link Extractor}) produced for
 * that embed. Both ids are composed by {@link DigestIdentifier#generateForEmbed} from
 * {@code content-hash || parentId || EMBEDDED_RELATIONSHIP_ID || resourceName}. For a PST/OST the
 * EMBEDDED_RELATIONSHIP_ID (Message-ID) and resourceName/Content-Type are populated DURING the
 * message sub-parse, so if retrieval freezes the id BEFORE that sub-parse (the bug) every message
 * -- and by parent-id cascade every attachment -- gets a different id and its raw bytes land under
 * an id datashare never polls (RawArtifact then finds no {@code raw}).
 *
 * <p>Uses the same multi-folder {@code testPST.pst} fixture the fan-out determinism test relies on,
 * driven with the production salted digester + DigestIdentifier so ids match exactly. RED before the
 * fix (id sets diverge: messages + attachments mismatch), GREEN after (id sets equal).
 */
public class EmbeddedDocumentExtractorPstIdParityTest {

    private static final String PST = "/documents/pst/testPST.pst";

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    private DocumentFactory factory() {
        return new DocumentFactory().withIdentifier(new DigestIdentifier("SHA-256", StandardCharsets.UTF_8));
    }

    private void collect(final TikaDocument doc, final Set<String> ids) {
        for (final EmbeddedTikaDocument embed : doc.getEmbeds()) {
            ids.add(embed.getId());
            collect(embed, ids);
        }
    }

    @Test(timeout = 300_000)
    public void retrieval_extractAll_ids_match_index_embed_ids() throws Exception {
        final Path pst = Paths.get(getClass().getResource(PST).toURI());
        final DocumentFactory factory = factory();

        // INDEX walk (EmbedSpawner): parse the PST serially, read the whole tree, then collect the
        // id of every embed at every level -- these are the ids datashare indexes and later polls.
        final TreeSet<String> indexIds = new TreeSet<>();
        try (Extractor extractor = new Extractor(factory)) {
            extractor.setDigester(new UpdatableDigester("prj", "SHA-256"));
            final TikaDocument indexRoot = extractor.extract(pst);
            try (Reader r = indexRoot.getReader()) { Spewer.toString(r); }
            collect(indexRoot, indexIds);
        }
        assertThat(indexIds).as("index produced embed ids").isNotEmpty();
        assertThat(indexIds.size()).as("fixture is genuinely multi-embed").isGreaterThan(1);

        // RETRIEVAL walk (EmbeddedDocumentExtractor.extractAll): re-extract every embed's bytes to
        // the artifact dir. A fresh root off the same file + factory has the same root id, so the
        // whole id chain is comparable. The written ids are the <id> dir names under artifactPath.
        final Path artifactPath = tmp.newFolder("artifacts").toPath();
        final TikaDocument retrievalRoot = factory.create(pst);
        new EmbeddedDocumentExtractor(new UpdatableDigester("prj", "SHA-256"), artifactPath).extractAll(retrievalRoot);

        final TreeSet<String> retrievalIds = new TreeSet<>();
        try (Stream<Path> walk = Files.walk(artifactPath)) {
            walk.filter(p -> p.getFileName().toString().equals("raw"))
                .forEach(raw -> retrievalIds.add(raw.getParent().getFileName().toString()));
        }

        // The two roots must agree on the root id, or nothing below could match.
        assertThat(retrievalRoot.getId()).as("root id parity").isNotNull();

        // The crux: retrieval must have written raw bytes under exactly the indexed embed ids.
        assertThat(retrievalIds).as("retrieval raw ids == index embed ids").isEqualTo(indexIds);
    }
}
