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

public class EmbedSpawnerSizeGuardTest {

    private TikaDocument root(final String path) {
        return new DocumentFactory()
                .withIdentifier(new DigestIdentifier("SHA-256", Charset.defaultCharset()))
                .create(Paths.get(path));
    }

    // Serial spawner with the depth guard disabled and an explicit, small decompressed-size cap and no
    // output path (no spooling), so only the size guard is under test.
    private EmbedSpawner spawnerWithSize(final TikaDocument root, final long maxEmbedSizeBytes) {
        return new EmbedSpawner(root, new ParseContext(), null,
                w -> new BodyContentHandler(w),
                64L * 1024 * 1024, new TemporaryResources(), () -> false, 0, maxEmbedSizeBytes);
    }

    private TikaDocument parent() {
        return new DocumentFactory()
                .withIdentifier(new DigestIdentifier("SHA-256", StandardCharsets.UTF_8))
                .create(Paths.get("/tmp/parent"));
    }

    private Metadata nonInline(final String name, final Long contentLength) {
        final Metadata m = new Metadata();
        m.set(org.apache.tika.metadata.TikaCoreProperties.RESOURCE_NAME_KEY, name);
        if (contentLength != null) {
            m.set(Metadata.CONTENT_LENGTH, Long.toString(contentLength));
        }
        return m;
    }

    @Test
    public void testEmbedBeyondSizeIsRefusedAndRecorded() throws Exception {
        final TikaDocument root = root("/tmp/fake-root.zip");
        final EmbedSpawner spawner = spawnerWithSize(root, 100L);
        final TikaDocument parent = parent();
        spawner.pushForTest(parent);

        // Declared decompressed size (1000) exceeds the 100-byte cap: the embed is refused.
        spawner.parseEmbedded(new ByteArrayInputStream("payload".getBytes(StandardCharsets.UTF_8)),
                new BodyContentHandler(), nonInline("bomb.bin", 1000L), false);

        // Refused: no embed added to the parent, and the skip is recorded on the parent.
        assertThat(parent.getEmbeds()).isEmpty();
        assertThat(parent.getMetadata().get(EmbedSpawner.EMBEDS_SKIPPED_MAX_SIZE)).isEqualTo("1");
    }

    @Test
    public void testSecondRefusedEmbedIncrementsParentMarker() throws Exception {
        final TikaDocument root = root("/tmp/fake-root.zip");
        final EmbedSpawner spawner = spawnerWithSize(root, 100L);
        final TikaDocument parent = parent();
        spawner.pushForTest(parent);

        spawner.parseEmbedded(new ByteArrayInputStream("a".getBytes(StandardCharsets.UTF_8)),
                new BodyContentHandler(), nonInline("bomb1.bin", 1000L), false);
        spawner.parseEmbedded(new ByteArrayInputStream("b".getBytes(StandardCharsets.UTF_8)),
                new BodyContentHandler(), nonInline("bomb2.bin", 2000L), false);

        assertThat(parent.getMetadata().get(EmbedSpawner.EMBEDS_SKIPPED_MAX_SIZE)).isEqualTo("2");
        assertThat(parent.getEmbeds()).isEmpty();
    }

    @Test
    public void testUnderCapEmbedStillExtracts() throws Exception {
        final TikaDocument root = root("/tmp/fake-root.zip");
        final EmbedSpawner spawner = spawnerWithSize(root, 100L);
        final TikaDocument parent = parent();
        spawner.pushForTest(parent);

        // Declared decompressed size (50) is under the 100-byte cap: the embed takes the normal spawn path.
        spawner.parseEmbedded(new ByteArrayInputStream("payload".getBytes(StandardCharsets.UTF_8)),
                new BodyContentHandler(), nonInline("real.bin", 50L), false);

        assertThat(parent.getMetadata().get(EmbedSpawner.EMBEDS_SKIPPED_MAX_SIZE)).isNull();
        assertThat(parent.getEmbeds()).isNotEmpty();
    }

    @Test
    public void testUnknownSizeIsNotGuarded() throws Exception {
        final TikaDocument root = root("/tmp/fake-root.zip");
        final EmbedSpawner spawner = spawnerWithSize(root, 100L);
        final TikaDocument parent = parent();
        spawner.pushForTest(parent);

        // No CONTENT_LENGTH on the embed: size is unknown, so the guard does not fire (do not guess).
        spawner.parseEmbedded(new ByteArrayInputStream("payload".getBytes(StandardCharsets.UTF_8)),
                new BodyContentHandler(), nonInline("unknown.bin", null), false);

        assertThat(parent.getMetadata().get(EmbedSpawner.EMBEDS_SKIPPED_MAX_SIZE)).isNull();
        assertThat(parent.getEmbeds()).isNotEmpty();
    }

    @Test
    public void testGuardDisabledWhenSizeIsZero() throws Exception {
        final TikaDocument root = root("/tmp/fake-root.zip");
        final EmbedSpawner spawner = spawnerWithSize(root, 0L); // disabled
        final TikaDocument parent = parent();
        spawner.pushForTest(parent);

        // Even a huge declared size is not refused when the cap is disabled (cap <= 0).
        spawner.parseEmbedded(new ByteArrayInputStream("payload".getBytes(StandardCharsets.UTF_8)),
                new BodyContentHandler(), nonInline("huge.bin", 10L * 1024 * 1024 * 1024), false);

        assertThat(parent.getMetadata().get(EmbedSpawner.EMBEDS_SKIPPED_MAX_SIZE)).isNull();
        assertThat(parent.getEmbeds()).isNotEmpty();
    }
}
