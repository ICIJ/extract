package org.icij.extract.extractor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.function.LongSupplier;

/** Default progress listener: logs one INFO line per in-flight file each heartbeat. */
public class LoggingProgressListener implements ProgressListener {
    private static final Logger logger = LoggerFactory.getLogger(LoggingProgressListener.class);
    private final LongSupplier clock;

    public LoggingProgressListener() { this(System::currentTimeMillis); }
    public LoggingProgressListener(final LongSupplier clock) { this.clock = clock; }

    static String formatLine(final ExtractionProgress p, final long now) {
        final long seconds = p.elapsedMillis(now) / 1000L;
        return String.format("%s: %ds, %d embeds, %d/%d OCR done, %d skipped(maxDepth), %d skipped(maxSize)",
                p.path(), seconds, p.embedsParsed(), p.ocrCompleted(), p.ocrSubmitted(),
                p.embedsSkippedMaxDepth(), p.embedsSkippedMaxSize());
    }

    @Override
    public void onHeartbeat(final Collection<ExtractionProgress> inFlight) {
        if (inFlight.isEmpty()) {
            return;
        }
        final long now = clock.getAsLong();
        for (final ExtractionProgress p : inFlight) {
            logger.info(formatLine(p, now));
        }
    }
}
