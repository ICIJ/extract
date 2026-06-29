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
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.fest.assertions.Assertions.assertThat;

public class EmbedSpawnerDeferredOcrTest {
    @Test public void testEligibleImageSubmitsOcrToExecutorAndDigestsSynchronously() throws Exception {
        TikaDocument root = new DocumentFactory().withIdentifier(new PathIdentifier())
            .create(java.nio.file.Paths.get("/root.eml"));
        ExecutorService ocr = Executors.newSingleThreadExecutor();
        ExtractionProgress progress = new ExtractionProgress(root.getPath(), 0L);
        DigestingParser.Digester digester = new CommonsDigester(20 * 1024 * 1024, "SHA256");

        try (TemporaryResources tmp = new TemporaryResources()) {
            EmbedSpawner spawner = new EmbedSpawner(
                root, new org.apache.tika.parser.ParseContext(), null,
                writer -> new org.apache.tika.sax.BodyContentHandler(writer),
                64L * 1024 * 1024, tmp, () -> false,
                ocr, progress, digester, /*ocrFanout*/ true, /*ocrMinImageBytes*/ 0L);

            Metadata m = new Metadata();
            m.set(Metadata.CONTENT_TYPE, "image/png");
            m.set(org.apache.tika.metadata.TikaCoreProperties.RESOURCE_NAME_KEY, "pic.png");

            spawner.parseEmbedded(
                new ByteArrayInputStream("PNG\r\n".getBytes(StandardCharsets.UTF_8)),
                new org.apache.tika.sax.BodyContentHandler(), m, false);

            assertThat(progress.ocrSubmitted()).isEqualTo(1L);
            // Drive the backstop: reading the embed's reader blocks until the OCR task completes.
            try (Reader r = root.getEmbeds().iterator().next().getReader()) {
                r.transferTo(java.io.Writer.nullWriter());
            }
            assertThat(progress.ocrCompleted()).isEqualTo(1L);
            // digest set synchronously on the embed metadata
            assertThat(root.getEmbeds().iterator().next().getMetadata()
                .get("X-TIKA:digest:SHA256")).isNotNull();
        } finally {
            ocr.shutdownNow();
        }
    }

    // Fix wave 4: an image embed the EmbeddedStreamTranslator WOULD translate (here: an image/png
    // whose TikaInputStream carries a POIFS DirectoryEntry open container, as for an image stored as
    // an OLE object inside a legacy .doc/.xls/.ppt) MUST NOT be deferred. Serial mode digests its
    // TRANSLATED bytes via Tika's DigestingParser, so it must take the serial inline path here too,
    // keeping the embed ID / digest / artifact filename byte-identical. Assert no OCR was submitted
    // (deferred path NOT taken) and that an embed was still added (serial path ran).
    @Test public void testTranslatableImageWithOpenContainerIsNotDeferred() throws Exception {
        TikaDocument root = new DocumentFactory().withIdentifier(new PathIdentifier())
            .create(java.nio.file.Paths.get("/root.doc"));
        ExecutorService ocr = Executors.newSingleThreadExecutor();
        ExtractionProgress progress = new ExtractionProgress(root.getPath(), 0L);
        DigestingParser.Digester digester = new CommonsDigester(20 * 1024 * 1024, "SHA256");

        try (TemporaryResources tmp = new TemporaryResources();
             org.apache.poi.poifs.filesystem.POIFSFileSystem poifs =
                 new org.apache.poi.poifs.filesystem.POIFSFileSystem()) {
            EmbedSpawner spawner = new EmbedSpawner(
                root, new org.apache.tika.parser.ParseContext(), null,
                writer -> new org.apache.tika.sax.BodyContentHandler(writer),
                64L * 1024 * 1024, tmp, () -> false,
                ocr, progress, digester, /*ocrFanout*/ true, /*ocrMinImageBytes*/ 0L);

            Metadata m = new Metadata();
            m.set(Metadata.CONTENT_TYPE, "image/png");
            m.set(org.apache.tika.metadata.TikaCoreProperties.RESOURCE_NAME_KEY, "ole-image.png");

            // A TikaInputStream with a POIFS DirectoryEntry open container -> MSEmbeddedStreamTranslator
            // (registered by tika-parser-microsoft-module) returns shouldTranslate()==true. Pass it as a
            // TikaInputStream so the spawner reuses it (TikaInputStream.get is idempotent) and the open
            // container survives into the shouldTranslate() check.
            org.apache.tika.io.TikaInputStream tis = org.apache.tika.io.TikaInputStream.get(
                new ByteArrayInputStream("PNG\r\n".getBytes(StandardCharsets.UTF_8)));
            tis.setOpenContainer(poifs.getRoot()); // DirectoryEntry

            spawner.parseEmbedded(tis, new org.apache.tika.sax.BodyContentHandler(), m, false);

            // Deferred path NOT taken: nothing submitted to the OCR pool.
            assertThat(progress.ocrSubmitted()).isEqualTo(0L);
            // Serial path ran: the embed was still added to the tree.
            assertThat(root.getEmbeds().size()).isEqualTo(1);
        } finally {
            ocr.shutdownNow();
        }
    }

    // Fix 2: if the OCR executor is shut down before parsing, submit() is rejected. The reader
    // backstop must NOT hang forever — it must return and the embed metadata must record the
    // exception. A short timeout turns a regression into a test failure rather than a suite hang.
    @Test(timeout = 10000)
    public void testRejectedOcrSubmitDoesNotHangReaderAndRecordsException() throws Exception {
        TikaDocument root = new DocumentFactory().withIdentifier(new PathIdentifier())
            .create(java.nio.file.Paths.get("/root.eml"));
        ExecutorService ocr = Executors.newSingleThreadExecutor();
        ocr.shutdownNow(); // submit will be rejected from now on
        ExtractionProgress progress = new ExtractionProgress(root.getPath(), 0L);
        DigestingParser.Digester digester = new CommonsDigester(20 * 1024 * 1024, "SHA256");

        try (TemporaryResources tmp = new TemporaryResources()) {
            EmbedSpawner spawner = new EmbedSpawner(
                root, new org.apache.tika.parser.ParseContext(), null,
                writer -> new org.apache.tika.sax.BodyContentHandler(writer),
                64L * 1024 * 1024, tmp, () -> false,
                ocr, progress, digester, /*ocrFanout*/ true, /*ocrMinImageBytes*/ 0L);

            Metadata m = new Metadata();
            m.set(Metadata.CONTENT_TYPE, "image/png");
            m.set(org.apache.tika.metadata.TikaCoreProperties.RESOURCE_NAME_KEY, "pic.png");

            spawner.parseEmbedded(
                new ByteArrayInputStream("PNG\r\n".getBytes(StandardCharsets.UTF_8)),
                new org.apache.tika.sax.BodyContentHandler(), m, false);

            // No future was scheduled and the submit was never counted (counters stay balanced).
            assertThat(progress.ocrSubmitted()).isEqualTo(0L);

            // The backstop must have been completed: reading the embed must return, not hang.
            TikaDocument embed = root.getEmbeds().iterator().next();
            embed.getReader().close();

            assertThat(embed.getMetadata()
                .get(org.apache.tika.metadata.TikaCoreProperties.TIKA_META_EXCEPTION_EMBEDDED_STREAM))
                .isNotNull();
        } finally {
            ocr.shutdownNow();
        }
    }
}
