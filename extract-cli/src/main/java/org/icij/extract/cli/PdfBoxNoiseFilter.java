package org.icij.extract.cli;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;

/**
 * Drops PDFBox/FontBox's high-volume font-fallback noise (dominated by "No current font",
 * ~5,556 lines on a single big-OST OCR pass) while leaving every other PDFBox/FontBox warning
 * intact in all appenders, so a genuinely malformed PDF still leaves a diagnostic trail. The
 * noise originates inside PDFBox, so message-level filtering is the only way to separate it from
 * real warnings on the same logger. Extend NOISE_SUBSTRINGS as the F5 measurement run confirms
 * additional dominant noise messages.
 */
public class PdfBoxNoiseFilter extends Filter<ILoggingEvent> {

    private static final String[] NOISE_SUBSTRINGS = { "No current font" };

    @Override
    public FilterReply decide(final ILoggingEvent event) {
        final String loggerName = event.getLoggerName();
        if (loggerName == null
                || (!loggerName.startsWith("org.apache.pdfbox") && !loggerName.startsWith("org.apache.fontbox"))) {
            return FilterReply.NEUTRAL;
        }
        final String message = event.getFormattedMessage();
        if (message == null) {
            return FilterReply.NEUTRAL;
        }
        for (final String noise : NOISE_SUBSTRINGS) {
            if (message.contains(noise)) {
                return FilterReply.DENY;
            }
        }
        return FilterReply.NEUTRAL;
    }
}
