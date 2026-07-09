package org.icij.extract.extractor;

import org.apache.tika.io.TemporaryResources;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.BodyContentHandler;
import org.icij.extract.document.DigestIdentifier;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.TikaDocument;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InterruptedIOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;

import static org.fest.assertions.Assertions.assertThat;

/**
 * When a parse is cancelled, Extractor's parse-timeout does {@code future.cancel(true)}, which
 * interrupts the parse thread. EmbedSpawner must treat that interrupt as a cancellation point and
 * abort at the next embedded document instead of continuing to produce embeds (which, once the spew
 * worker has stopped draining, accumulate and drive the process into OOM).
 */
public class EmbedSpawnerCancellationTest {

    private TikaDocument root(final String path) {
        return new DocumentFactory()
                .withIdentifier(new DigestIdentifier("SHA-256", Charset.defaultCharset()))
                .create(Paths.get(path));
    }

    private EmbedSpawner serialSpawner(final TikaDocument root) {
        // depth guard disabled (0) and no output path, so parseEmbedded reaches the spawn path
        // unless the cancellation check short-circuits it first.
        return new EmbedSpawner(root, new ParseContext(), null,
                w -> new BodyContentHandler(w),
                64L * 1024 * 1024, new TemporaryResources(), () -> false, 0);
    }

    private Metadata nonInline(final String name) {
        final Metadata m = new Metadata();
        m.set(TikaCoreProperties.RESOURCE_NAME_KEY, name);
        return m;
    }

    @Test
    public void testInterruptedParseAbortsBeforeSpawningEmbed() throws Exception {
        final TikaDocument root = root("/tmp/fake-root.pst");
        final EmbedSpawner spawner = serialSpawner(root);
        final TikaDocument parent = new DocumentFactory()
                .withIdentifier(new DigestIdentifier("SHA-256", StandardCharsets.UTF_8))
                .create(Paths.get("/tmp/parent"));
        spawner.pushForTest(parent);

        Thread.currentThread().interrupt(); // simulate future.cancel(true) on parse-timeout
        boolean threw = false;
        try {
            spawner.parseEmbedded(new ByteArrayInputStream("payload".getBytes(StandardCharsets.UTF_8)),
                    new BodyContentHandler(), nonInline("attachment.bin"), false);
        } catch (final InterruptedIOException expected) {
            threw = true;
            // The interrupt flag must stay set so any subsequent embed on this parse also aborts,
            // even if a Tika container parser swallows an individual throw.
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted(); // clear so we don't poison the test-runner thread
        }

        assertThat(threw).isTrue();
        // No embed was spawned: the parse aborted before doing any work.
        assertThat(parent.getEmbeds()).isEmpty();
    }
}
