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

    @Test
    public void testForkCountsDepthAbsolutely() throws Exception {
        final TikaDocument root = root("/tmp/fake-root.pst");
        final EmbedSpawner base = spawnerWithDepth(root, 3);
        // Push two docs so the base's stack size becomes 3 (simulating a PST reached at absolute depth 3).
        final TikaDocument folder = new DocumentFactory()
                .withIdentifier(new DigestIdentifier("SHA-256", StandardCharsets.UTF_8))
                .create(Paths.get("/tmp/folder"));
        final TikaDocument pst = new DocumentFactory()
                .withIdentifier(new DigestIdentifier("SHA-256", StandardCharsets.UTF_8))
                .create(Paths.get("/tmp/pst"));
        base.pushForTest(folder);
        base.pushForTest(pst);

        final EmbedSpawner forked = base.fork();
        assertThat(forked.baseDepthOffsetForTest()).isEqualTo(2);

        // Fork stack size 2 (root + one pushed doc) -> absolute depth 2 + 2 = 4 > 3: refused.
        final TikaDocument message = new DocumentFactory()
                .withIdentifier(new DigestIdentifier("SHA-256", StandardCharsets.UTF_8))
                .create(Paths.get("/tmp/message"));
        forked.pushForTest(message);

        forked.parseEmbedded(new ByteArrayInputStream("payload".getBytes(StandardCharsets.UTF_8)),
                new BodyContentHandler(), nonInline("attachment.bin"), false);

        assertThat(message.getMetadata().get(EmbedSpawner.EMBEDS_SKIPPED_MAX_DEPTH)).isEqualTo("1");

        // Sibling fork: stack size 1 (root only) -> absolute depth 2 + 1 = 3, NOT > 3: not refused.
        final EmbedSpawner forkedSibling = base.fork();
        assertThat(forkedSibling.baseDepthOffsetForTest()).isEqualTo(2);

        forkedSibling.parseEmbedded(new ByteArrayInputStream("payload".getBytes(StandardCharsets.UTF_8)),
                new BodyContentHandler(), nonInline("attachment2.bin"), false);

        assertThat(pst.getMetadata().get(EmbedSpawner.EMBEDS_SKIPPED_MAX_DEPTH)).isNull();
    }

    @Test
    public void testWarnLogsOncePerParentAcrossRefusals() throws Exception {
        final ch.qos.logback.classic.Logger log =
                (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(EmbedParser.class);
        final ch.qos.logback.classic.Level originalLevel = log.getLevel();
        final ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> appender =
                new ch.qos.logback.core.read.ListAppender<>();
        appender.start();
        log.addAppender(appender);
        log.setLevel(ch.qos.logback.classic.Level.INFO);

        try {
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

            final long warnCount = appender.list.stream()
                    .filter(e -> e.getLevel() == ch.qos.logback.classic.Level.WARN)
                    .filter(e -> e.getFormattedMessage().contains("beyond max depth"))
                    .count();
            assertThat(warnCount).isEqualTo(1L);
        } finally {
            log.detachAppender(appender);
            if (originalLevel != null) {
                log.setLevel(originalLevel);
            }
        }
    }
}
