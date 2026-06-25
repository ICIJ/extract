package org.icij.extract.cli;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.spi.FilterReply;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

public class PdfBoxNoiseFilterTest {

    private final PdfBoxNoiseFilter filter = new PdfBoxNoiseFilter();
    private final LoggerContext context = new LoggerContext();

    private LoggingEvent event(final String loggerName, final String message) {
        final Logger logger = context.getLogger(loggerName);
        return new LoggingEvent(Logger.class.getName(), logger, Level.WARN, message, null, null);
    }

    @Test
    public void denies_pdfBoxNoCurrentFontNoise() {
        assertThat(filter.decide(event("org.apache.pdfbox.contentstream.PDFStreamEngine", "No current font, will use default")))
                .isEqualTo(FilterReply.DENY);
    }

    @Test
    public void keeps_otherPdfBoxWarning() {
        assertThat(filter.decide(event("org.apache.pdfbox.pdmodel.PDDocument", "This PDF is malformed")))
                .isEqualTo(FilterReply.NEUTRAL);
    }

    @Test
    public void ignores_nonPdfBoxEvent() {
        assertThat(filter.decide(event("org.icij.extract.SomethingElse", "No current font")))
                .isEqualTo(FilterReply.NEUTRAL);
    }
}
