package org.icij.extract.extractor;

import org.junit.Test;
import org.junit.After;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.core.read.ListAppender;
import org.slf4j.LoggerFactory;
import static org.fest.assertions.Assertions.assertThat;

public class LoggingProgressListenerTest {
    @Test public void testFormatLine() {
        ExtractionProgress p = new ExtractionProgress(Paths.get("/x.ost"), 0L);
        p.incrementEmbeds(); p.incrementEmbeds();
        p.incrementOcrSubmitted(); p.incrementOcrSubmitted(); p.incrementOcrSubmitted();
        p.incrementOcrCompleted();
        assertThat(LoggingProgressListener.formatLine(p, 3_000L))
            .isEqualTo("/x.ost: 3s, 2 embeds, 1/3 OCR done, 0 skipped(maxDepth), 0 skipped(maxSize)");
    }

    @Test public void testFormatLineIncludesSkippedCount() {
        ExtractionProgress p = new ExtractionProgress(Paths.get("/x.ost"), 1_000L);
        p.incrementEmbeds();
        p.incrementEmbedsSkippedMaxDepth();
        assertThat(LoggingProgressListener.formatLine(p, 3_000L)).contains("1 skipped(maxDepth)");
    }

    @Test public void testFormatLineIncludesSkippedMaxSizeCount() {
        ExtractionProgress p = new ExtractionProgress(Paths.get("/x.ost"), 1_000L);
        p.incrementEmbeds();
        p.incrementEmbedsSkippedMaxSize();
        assertThat(LoggingProgressListener.formatLine(p, 3_000L)).contains("1 skipped(maxSize)");
    }

    @Test public void testEmptyInFlightLogsNothing() {
        Logger log = (Logger) LoggerFactory.getLogger(LoggingProgressListener.class);
        ch.qos.logback.classic.Level originalLevel = log.getLevel();
        ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> appender =
            new ListAppender<>();
        appender.start();
        log.addAppender(appender);
        log.setLevel(ch.qos.logback.classic.Level.INFO);

        LoggingProgressListener listener = new LoggingProgressListener(() -> 5000L);
        listener.onHeartbeat(new ArrayList<>());

        log.detachAppender(appender);
        if (originalLevel != null) {
            log.setLevel(originalLevel);
        }
        assertThat(appender.list).isEmpty();
    }

    @Test public void testLogsOneLinePerInFlightFile() {
        Logger log = (Logger) LoggerFactory.getLogger(LoggingProgressListener.class);
        ch.qos.logback.classic.Level originalLevel = log.getLevel();
        ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> appender =
            new ListAppender<>();
        appender.start();
        log.addAppender(appender);
        log.setLevel(ch.qos.logback.classic.Level.INFO);

        LoggingProgressListener listener = new LoggingProgressListener(() -> 5000L);
        List<ExtractionProgress> inFlight = new ArrayList<>();
        inFlight.add(new ExtractionProgress(Paths.get("/file1.ost"), 0L));
        inFlight.add(new ExtractionProgress(Paths.get("/file2.pst"), 0L));
        listener.onHeartbeat(inFlight);

        log.detachAppender(appender);
        if (originalLevel != null) {
            log.setLevel(originalLevel);
        }
        assertThat(appender.list).hasSize(2);
    }

    @Test public void testFormatLineShowsEstimateWhenUnitsKnown() {
        ExtractionProgress p = new ExtractionProgress(Paths.get("/foo.ost"), 0L);
        for (int i = 0; i < 300; i++) p.incrementEmbeds();   // 300 embeds so far
        for (int i = 0; i < 3; i++) p.incrementUnits();      // 3 of ...
        p.setExpectedUnits(4L);                              // ... 4 units => 75%
        // estTotal = round(300 * 4 / 3) = 400
        assertThat(LoggingProgressListener.formatLine(p, 1_000L))
            .isEqualTo("/foo.ost: 1s, 300/~400 embeds (~75%), 0/0 OCR done, 0 skipped(maxDepth), 0 skipped(maxSize)");
    }

    @Test public void testFormatLineFallsBackWhenUnitsUnknown() {
        ExtractionProgress p = new ExtractionProgress(Paths.get("/logs.tar.gz"), 0L);
        p.incrementEmbeds();
        assertThat(LoggingProgressListener.formatLine(p, 1_000L))
            .isEqualTo("/logs.tar.gz: 1s, 1 embeds, 0/0 OCR done, 0 skipped(maxDepth), 0 skipped(maxSize)");
    }

    @Test public void testFormatLineFallsBackBeforeFirstUnit() {
        ExtractionProgress p = new ExtractionProgress(Paths.get("/foo.ost"), 0L);
        p.incrementEmbeds();
        p.setExpectedUnits(10L);   // total known, but unitsParsed == 0 -> no divide, fall back
        assertThat(LoggingProgressListener.formatLine(p, 1_000L)).contains("1 embeds,");
    }
}
