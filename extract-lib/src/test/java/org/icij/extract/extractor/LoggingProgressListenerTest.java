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
            .isEqualTo("/x.ost: 3s, 2 embeds, 1/3 OCR done");
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
}
