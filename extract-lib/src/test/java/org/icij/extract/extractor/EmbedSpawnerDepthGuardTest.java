package org.icij.extract.extractor;

import org.apache.tika.io.TemporaryResources;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.BodyContentHandler;
import org.icij.extract.document.DigestIdentifier;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.TikaDocument;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;

import static org.fest.assertions.Assertions.assertThat;

public class EmbedSpawnerDepthGuardTest {

    private TikaDocument root(final String path) {
        return new DocumentFactory()
                .withIdentifier(new DigestIdentifier("SHA-256", Charset.defaultCharset()))
                .create(Paths.get(path));
    }

    // Serial spawner with an explicit, small depth limit and no output path (no spooling).
    private EmbedSpawner spawnerWithDepth(final TikaDocument root, final int maxEmbedDepth) {
        return new EmbedSpawner(root, new ParseContext(), null,
                w -> new BodyContentHandler(w),
                64L * 1024 * 1024, new TemporaryResources(), () -> false, maxEmbedDepth);
    }

    private Metadata nonInline(final String name) {
        final Metadata m = new Metadata();
        m.set(org.apache.tika.metadata.TikaCoreProperties.RESOURCE_NAME_KEY, name);
        return m;
    }

    @Test
    public void testEmbedBeyondDepthIsRefusedAndRecorded() throws Exception {
        final TikaDocument root = root("/tmp/fake-root.zip");
        final EmbedSpawner spawner = spawnerWithDepth(root, 1);
        // Push one document so the stack size becomes 2 (> maxEmbedDepth of 1): the next embed is refused.
        final TikaDocument parent = new DocumentFactory()
                .withIdentifier(new DigestIdentifier("SHA-256", StandardCharsets.UTF_8))
                .create(Paths.get("/tmp/parent"));
        spawner.pushForTest(parent);

        spawner.parseEmbedded(new ByteArrayInputStream("payload".getBytes(StandardCharsets.UTF_8)),
                new BodyContentHandler(), nonInline("bomb.bin"), false);

        // Refused: no embed added to the parent, and the skip is recorded on the parent + progress-free path.
        assertThat(parent.getEmbeds()).isEmpty();
        assertThat(parent.getMetadata().get(EmbedSpawner.EMBEDS_SKIPPED_MAX_DEPTH)).isEqualTo("1");
    }

    @Test
    public void testSecondRefusedEmbedIncrementsParentMarker() throws Exception {
        final TikaDocument root = root("/tmp/fake-root.zip");
        final EmbedSpawner spawner = spawnerWithDepth(root, 1);
        final TikaDocument parent = new DocumentFactory()
                .withIdentifier(new DigestIdentifier("SHA-256", StandardCharsets.UTF_8))
                .create(Paths.get("/tmp/parent"));
        spawner.pushForTest(parent);

        spawner.parseEmbedded(new ByteArrayInputStream("a".getBytes(StandardCharsets.UTF_8)),
                new BodyContentHandler(), nonInline("bomb1.bin"), false);
        spawner.parseEmbedded(new ByteArrayInputStream("b".getBytes(StandardCharsets.UTF_8)),
                new BodyContentHandler(), nonInline("bomb2.bin"), false);

        assertThat(parent.getMetadata().get(EmbedSpawner.EMBEDS_SKIPPED_MAX_DEPTH)).isEqualTo("2");
        assertThat(parent.getEmbeds()).isEmpty();
    }

    @Test
    public void testGuardDisabledWhenDepthIsZero() throws Exception {
        final TikaDocument root = root("/tmp/fake-root.zip");
        final EmbedSpawner spawner = spawnerWithDepth(root, 0); // disabled
        final TikaDocument parent = new DocumentFactory()
                .withIdentifier(new DigestIdentifier("SHA-256", StandardCharsets.UTF_8))
                .create(Paths.get("/tmp/parent"));
        spawner.pushForTest(parent);

        spawner.parseEmbedded(new ByteArrayInputStream("payload".getBytes(StandardCharsets.UTF_8)),
                new BodyContentHandler(), nonInline("real.bin"), false);

        // Not refused: nothing recorded on the parent (the embed took the normal spawn path).
        assertThat(parent.getMetadata().get(EmbedSpawner.EMBEDS_SKIPPED_MAX_DEPTH)).isNull();
    }
}
