package org.icij.extract.extractor;

import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.PathIdentifier;
import org.icij.extract.document.TikaDocument;
import org.icij.extract.report.HashMapReportMap;
import org.icij.extract.report.Report;
import org.icij.extract.report.Reporter;
import org.icij.spewer.FieldNames;
import org.icij.spewer.PrintStreamSpewer;
import org.icij.spewer.Spewer;
import org.junit.After;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;

import static org.fest.assertions.Assertions.assertThat;

public class ExtractorParseTimeoutTest {

    /** Never releases on its own — a deterministic hang that only an interrupt can end. */
    private final CountDownLatch latch = new CountDownLatch(1);

    @After
    public void releaseLatch() {
        latch.countDown();
    }

    /** A Reader whose read() blocks until interrupted (or the latch is released in teardown). */
    private class BlockingReader extends Reader {
        @Override
        public int read(final char[] cbuf, final int off, final int len) throws IOException {
            try {
                latch.await();
                return -1;
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted", e);
            }
        }

        @Override
        public void close() {
        }
    }

    /** Extractor whose lazy extract(Path) returns a document backed by a hanging reader. */
    private class HangingExtractor extends Extractor {
        @Override
        public TikaDocument extract(final Path path) {
            final TikaDocument document =
                    new DocumentFactory().withIdentifier(new PathIdentifier()).create(path);
            document.setReader(new BlockingReader());
            return document;
        }
    }

    private Spewer nullSpewer() {
        return new PrintStreamSpewer(
                new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8), new FieldNames());
    }

    @Test(timeout = 10_000)
    public void testHungParseRecordsFailureTimeoutAndReturns() {
        final HashMapReportMap reportMap = new HashMapReportMap();
        final Reporter reporter = new Reporter(reportMap);
        final Path path = Paths.get("hang");

        final Extractor extractor = new HangingExtractor();
        extractor.setParseTimeout(Duration.ofMillis(200));

        // Must return (slot freed), not throw, not hang.
        extractor.extract(path, nullSpewer(), reporter);

        final Report report = reportMap.get(path);
        assertThat(report).isNotNull();
        assertThat(report.getStatus()).isEqualTo(ExtractionStatus.FAILURE_TIMEOUT);
    }
}
