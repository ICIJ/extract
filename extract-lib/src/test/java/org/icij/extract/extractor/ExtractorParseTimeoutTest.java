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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;

import static org.fest.assertions.Assertions.assertThat;

public class ExtractorParseTimeoutTest {

    /** Never releases on its own — a deterministic hang that only an interrupt can end. */
    private final CountDownLatch latch = new CountDownLatch(1);

    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

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

    @Test(timeout = 10_000)
    public void testDisabledTimeoutExtractsNormally() throws IOException {
        final Path file = folder.newFile("hello.txt").toPath();
        Files.write(file, "hello world".getBytes(StandardCharsets.UTF_8));

        final HashMapReportMap reportMap = new HashMapReportMap();
        final Reporter reporter = new Reporter(reportMap);

        final Extractor extractor = new Extractor();
        extractor.setParseTimeout(Duration.ZERO);   // disabled -> runs doExtract directly

        extractor.extract(file, nullSpewer(), reporter);

        final Report report = reportMap.get(file);
        assertThat(report).isNotNull();
        assertThat(report.getStatus()).isEqualTo(ExtractionStatus.SUCCESS);
    }

    /** Extractor whose lazy extract(Path) throws a plain IOException from the parse path. */
    private static class IOExceptionExtractor extends Extractor {
        @Override
        public TikaDocument extract(final Path path) throws IOException {
            throw new IOException("boom");
        }
    }

    @Test(timeout = 10_000)
    public void testNormalIOExceptionStillClassifiedAsUnreadable() {
        final HashMapReportMap reportMap = new HashMapReportMap();
        final Reporter reporter = new Reporter(reportMap);
        final Path path = Paths.get("broken");

        final Extractor extractor = new IOExceptionExtractor();
        extractor.setParseTimeout(Duration.ofMinutes(5));   // watchdog ON, but no timeout fires

        extractor.extract(path, nullSpewer(), reporter);

        final Report report = reportMap.get(path);
        assertThat(report).isNotNull();
        // Plain IOException with no TikaException cause -> FAILURE_UNREADABLE, same as before the watchdog.
        assertThat(report.getStatus()).isEqualTo(ExtractionStatus.FAILURE_UNREADABLE);
    }

    /** Extractor whose lazy extract(Path) throws an OutOfMemoryError from the parse path. */
    private static class OOMExtractor extends Extractor {
        private final OutOfMemoryError oom;

        OOMExtractor(final OutOfMemoryError oom) {
            this.oom = oom;
        }

        @Override
        public TikaDocument extract(final Path path) throws IOException {
            throw oom;
        }
    }

    @Test(timeout = 10_000)
    public void testFatalErrorPropagatesVerbatimThroughWatchdog() {
        final HashMapReportMap reportMap = new HashMapReportMap();
        final Reporter reporter = new Reporter(reportMap);
        final Path path = Paths.get("fatal");
        final OutOfMemoryError oom = new OutOfMemoryError("synthetic");

        final Extractor extractor = new OOMExtractor(oom);
        extractor.setParseTimeout(Duration.ofMinutes(5));   // watchdog ON, will not fire

        Throwable caught = null;
        try {
            extractor.extract(path, nullSpewer(), reporter);
        } catch (final Throwable t) {
            caught = t;
        }

        // The SAME OOM instance must escape extract(Path, Spewer, Reporter) verbatim.
        assertThat(caught).isSameAs(oom);
        final Report report = reportMap.get(path);
        assertThat(report).isNotNull();
        assertThat(report.getStatus()).isEqualTo(ExtractionStatus.FAILURE_FATAL);
    }
}
