package org.icij.extract.extractor;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
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

public class EmbedSpawnerUnitCountTest {

    private TikaDocument root(final String path) {
        return new DocumentFactory()
                .withIdentifier(new DigestIdentifier("SHA-256", Charset.defaultCharset()))
                .create(Paths.get(path));
    }

    private Metadata nonInline(final String name) {
        final Metadata m = new Metadata();
        m.set(TikaCoreProperties.RESOURCE_NAME_KEY, name);
        return m;
    }

    @Test public void testDepthOneEmbedIncrementsUnits() throws Exception {
        final ExtractionProgress progress = new ExtractionProgress(Paths.get("/big.zip"), 0L);
        final TikaDocument root = root("/big.zip");
        // Stack holds only the root -> the next embed is at depth 1.
        final EmbedSpawner spawner = new EmbedSpawner(root, progress, 0);

        spawner.parseEmbedded(new ByteArrayInputStream("payload".getBytes(StandardCharsets.UTF_8)),
                new BodyContentHandler(), nonInline("entry1.txt"), false);

        assertThat(progress.unitsParsed()).isEqualTo(1L);
    }

    @Test public void testParserTrackedProgressIsNotDoubleCounted() throws Exception {
        final ExtractionProgress progress = new ExtractionProgress(Paths.get("/foo.ost"), 0L);
        progress.markParserTracksUnits(); // PST owns the numerator
        final TikaDocument root = root("/foo.ost");
        final EmbedSpawner spawner = new EmbedSpawner(root, progress, 0);

        spawner.parseEmbedded(new ByteArrayInputStream("msg".getBytes(StandardCharsets.UTF_8)),
                new BodyContentHandler(), nonInline("message1.eml"), false);

        assertThat(progress.unitsParsed()).isEqualTo(0L); // EmbedSpawner must not count when parser owns
    }
}
