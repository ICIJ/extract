package org.icij.extract.extractor;

import org.apache.tika.io.TemporaryResources;
import org.apache.tika.parser.ParseContext;
import org.icij.extract.document.DigestIdentifier;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.TikaDocument;
import org.junit.Test;

import java.nio.charset.Charset;
import java.nio.file.Paths;

import static org.fest.assertions.Assertions.assertThat;

public class EmbedSpawnerForkTest {

    private EmbedSpawner newSerialSpawner() {
        final TikaDocument root = new DocumentFactory()
                .withIdentifier(new DigestIdentifier("SHA-256", Charset.defaultCharset()))
                .create(Paths.get("/tmp/fake-root.ost"));
        // Serial constructor: no fan-out, no OCR. Sufficient to exercise fork()'s field sharing.
        return new EmbedSpawner(root, new ParseContext(), null,
                w -> new org.apache.tika.sax.BodyContentHandler(w),
                64L * 1024 * 1024, new TemporaryResources(), () -> false);
    }

    @Test
    public void testForkSharesBudgetButNotStack() {
        final EmbedSpawner base = newSerialSpawner();
        final EmbedSpawner forked = base.fork();

        // Same shared budget instance: a reservation on one is visible to the other.
        assertThat(forked.reservedBudget()).isSameAs(base.reservedBudget());

        // Independent DFS stacks: pushing on the fork must not change the base's stack depth.
        assertThat(forked.stackDepth()).isEqualTo(1); // seeded with root only
        assertThat(base.stackDepth()).isEqualTo(1);
    }

    @Test
    public void testForkContextRegistersSelfAndOmitsFanout() {
        final org.apache.tika.parser.ParseContext base = new org.apache.tika.parser.ParseContext();
        // The base context carries the fan-out config (this is what makes the OUTERMOST PST fan out).
        base.set(org.icij.extract.parser.PstFanoutConfig.class,
                new org.icij.extract.parser.PstFanoutConfig(true, () -> null));
        final org.icij.extract.document.TikaDocument root =
                new org.icij.extract.document.DocumentFactory()
                        .withIdentifier(new org.icij.extract.document.DigestIdentifier("SHA-384",
                                java.nio.charset.StandardCharsets.UTF_8))
                        .create(java.nio.file.Paths.get("/tmp/fake-root.ost"));
        final EmbedSpawner baseSpawner = new EmbedSpawner(root, base, null,
                w -> new org.apache.tika.sax.BodyContentHandler(w),
                64L * 1024 * 1024, new org.apache.tika.io.TemporaryResources(), () -> false);
        final EmbedSpawner forked = baseSpawner.fork();

        // The fork registers ITSELF as the extractor so nested embeds recurse into the fork.
        assertThat(forked.parseContextForTest()
                .get(org.apache.tika.extractor.EmbeddedDocumentExtractor.class)).isSameAs(forked);
        // The fork context OMITS PstFanoutConfig, so a nested PST attachment walks serially
        // (no reentrant pstParseExecutor starvation).
        assertThat(forked.parseContextForTest()
                .get(org.icij.extract.parser.PstFanoutConfig.class)).isNull();
    }
}
