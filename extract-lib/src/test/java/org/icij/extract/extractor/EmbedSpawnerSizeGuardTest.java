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

    // Serial spawner with the depth guard disabled and NO explicit size cap, so it carries the shipped
    // default (DEFAULT_MAX_EMBED_SIZE_BYTES). Used to assert the guard is off unless opted into.
    private EmbedSpawner spawnerWithDefaultSize(final TikaDocument root) {
        return new EmbedSpawner(root, new ParseContext(), null,
                w -> new BodyContentHandler(w),
                64L * 1024 * 1024, new TemporaryResources(), () -> false, 0);
    }

    private TikaDocument parent() {
        return new DocumentFactory()
                .withIdentifier(new DigestIdentifier("SHA-256", StandardCharsets.UTF_8))
                .create(Paths.get("/tmp/parent"));
    }

    private Metadata nonInline(final String name, final Long contentLength) {
        return nonInlineRaw(name, contentLength == null ? null : Long.toString(contentLength));
    }

    // Same as nonInline but sets CONTENT_LENGTH to a raw string, so tests can exercise values that are not
    // representable as a Java long (e.g. a crafted ZIP64 size above Long.MAX_VALUE) or non-numeric garbage.
    private Metadata nonInlineRaw(final String name, final String rawContentLength) {
        final Metadata m = new Metadata();
        m.set(org.apache.tika.metadata.TikaCoreProperties.RESOURCE_NAME_KEY, name);
        if (rawContentLength != null) {
            m.set(Metadata.CONTENT_LENGTH, rawContentLength);
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
    public void testDeclaredSizeAboveLongRangeIsRefused() throws Exception {
        final TikaDocument root = root("/tmp/fake-root.zip");
        final EmbedSpawner spawner = spawnerWithSize(root, 100L);
        final TikaDocument parent = parent();
        spawner.pushForTest(parent);

        // A crafted ZIP64 entry can declare an uncompressed size above Long.MAX_VALUE. Such a value is not
        // "unknown" — it is unambiguously enormous — so it must be refused, not fail open past Long.parseLong.
        final String aboveLongMax = "18446744073709551615"; // 2^64 - 1, > Long.MAX_VALUE
        spawner.parseEmbedded(new ByteArrayInputStream("payload".getBytes(StandardCharsets.UTF_8)),
                new BodyContentHandler(), nonInlineRaw("zip64-bomb.bin", aboveLongMax), false);

        assertThat(parent.getMetadata().get(EmbedSpawner.EMBEDS_SKIPPED_MAX_SIZE)).isEqualTo("1");
        assertThat(parent.getEmbeds()).isEmpty();
    }

    @Test
    public void testNonNumericSizeIsNotGuarded() throws Exception {
        final TikaDocument root = root("/tmp/fake-root.zip");
        final EmbedSpawner spawner = spawnerWithSize(root, 100L);
        final TikaDocument parent = parent();
        spawner.pushForTest(parent);

        // A genuinely non-numeric CONTENT_LENGTH is unknown (not a huge number), so the guard does not fire:
        // we do not guess, matching the absent-length behaviour.
        spawner.parseEmbedded(new ByteArrayInputStream("payload".getBytes(StandardCharsets.UTF_8)),
                new BodyContentHandler(), nonInlineRaw("weird.bin", "not-a-number"), false);

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

    @Test
    public void testGuardDisabledByDefault() throws Exception {
        final TikaDocument root = root("/tmp/fake-root.zip");
        // No explicit cap: the spawner carries the shipped default, which is disabled (opt-in guard).
        final EmbedSpawner spawner = spawnerWithDefaultSize(root);
        final TikaDocument parent = parent();
        spawner.pushForTest(parent);

        // The default must be disabled, so even a huge declared size is extracted, not skipped: a
        // legitimate multi-GiB attachment is never silently dropped unless an operator opts in.
        assertThat(spawner.maxEmbedSizeBytesForTest()).isEqualTo(0L);
        spawner.parseEmbedded(new ByteArrayInputStream("payload".getBytes(StandardCharsets.UTF_8)),
                new BodyContentHandler(), nonInline("huge.bin", 10L * 1024 * 1024 * 1024), false);

        assertThat(parent.getMetadata().get(EmbedSpawner.EMBEDS_SKIPPED_MAX_SIZE)).isNull();
        assertThat(parent.getEmbeds()).isNotEmpty();
    }
}
