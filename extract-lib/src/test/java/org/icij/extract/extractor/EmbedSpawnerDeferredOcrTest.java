package org.icij.extract.extractor;

import org.apache.tika.io.TemporaryResources;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.DigestingParser;
import org.apache.tika.parser.digestutils.CommonsDigester;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.PathIdentifier;
import org.icij.extract.document.TikaDocument;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.fest.assertions.Assertions.assertThat;

public class EmbedSpawnerDeferredOcrTest {
    @Test public void testEligibleImageSubmitsOcrToExecutorAndDigestsSynchronously() throws Exception {
        TikaDocument root = new DocumentFactory().withIdentifier(new PathIdentifier())
            .create(java.nio.file.Paths.get("/root.eml"));
        ExecutorService ocr = Executors.newSingleThreadExecutor();
        List<Future<?>> futures = new ArrayList<>();
        ExtractionProgress progress = new ExtractionProgress(root.getPath(), 0L);
        DigestingParser.Digester digester = new CommonsDigester(20 * 1024 * 1024, "SHA256");

        try (TemporaryResources tmp = new TemporaryResources()) {
            EmbedSpawner spawner = new EmbedSpawner(
                root, new org.apache.tika.parser.ParseContext(), null,
                writer -> new org.apache.tika.sax.BodyContentHandler(writer),
                64L * 1024 * 1024, tmp, () -> false,
                ocr, futures, progress, digester, /*ocrFanout*/ true, /*ocrMinImageBytes*/ 0L);

            Metadata m = new Metadata();
            m.set(Metadata.CONTENT_TYPE, "image/png");
            m.set(org.apache.tika.metadata.TikaCoreProperties.RESOURCE_NAME_KEY, "pic.png");

            spawner.parseEmbedded(
                new ByteArrayInputStream("PNG\r\n".getBytes(StandardCharsets.UTF_8)),
                new org.apache.tika.sax.BodyContentHandler(), m, false);

            assertThat(progress.ocrSubmitted()).isEqualTo(1L);
            assertThat(futures).hasSize(1);
            futures.get(0).get();                 // join
            assertThat(progress.ocrCompleted()).isEqualTo(1L);
            // digest set synchronously on the embed metadata
            assertThat(root.getEmbeds().iterator().next().getMetadata()
                .get("X-TIKA:digest:SHA256")).isNotNull();
        } finally {
            ocr.shutdownNow();
        }
    }
}
