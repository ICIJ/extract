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
        return String.format("%s: %ds, %s, %d/%d OCR done, %d skipped(maxDepth), %d skipped(maxSize)",
                p.path(), seconds, embedSegment(p), p.ocrCompleted(), p.ocrSubmitted(),
                p.embedsSkippedMaxDepth(), p.embedsSkippedMaxSize());
    }

    // Embed count with a projected total + percentage when the container exposes a cheap unit total
    // (PST messages, archive entries); otherwise the plain running count. The total is a projection
    // (embeds scale by the observed embeds-per-unit ratio), hence the '~'; the percentage is the real
    // unitsParsed/expectedUnits ground truth and never exceeds 100%.
    private static String embedSegment(final ExtractionProgress p) {
        final long embeds = p.embedsParsed();
        final long expectedUnits = p.expectedUnits();
        final long unitsParsed = p.unitsParsed();
        if (expectedUnits <= 0 || unitsParsed <= 0) {
            return String.format("%d embeds", embeds);
        }
        final long estTotal = Math.max(embeds, Math.round((double) embeds * expectedUnits / unitsParsed));
        final long pct = Math.min(100L, unitsParsed * 100L / expectedUnits);
        return String.format("%d/~%d embeds (~%d%%)", embeds, estTotal, pct);
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
