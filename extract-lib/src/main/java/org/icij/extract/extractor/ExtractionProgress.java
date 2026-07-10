package org.icij.extract.extractor;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

/** Mutable, thread-safe per-file extraction progress tracker. */
public class ExtractionProgress {
    private final Path path;
    private final long startMillis;
    private final AtomicLong embeds = new AtomicLong();
    private final AtomicLong ocrSubmitted = new AtomicLong();
    private final AtomicLong ocrCompleted = new AtomicLong();
    private final AtomicLong embedsSkippedMaxDepth = new AtomicLong();
    private final AtomicLong embedsSkippedMaxSize = new AtomicLong();
    private final AtomicLong expectedUnits = new AtomicLong(-1L);
    private final AtomicLong unitsParsed = new AtomicLong();
    private volatile boolean parserTracksUnits = false;

    public ExtractionProgress(final Path path, final long startMillis) {
        this.path = path;
        this.startMillis = startMillis;
    }

    public Path path() { return path; }
    public long startMillis() { return startMillis; }
    public long embedsParsed() { return embeds.get(); }
    public long ocrSubmitted() { return ocrSubmitted.get(); }
    public long ocrCompleted() { return ocrCompleted.get(); }
    public long embedsSkippedMaxDepth() { return embedsSkippedMaxDepth.get(); }
    public long embedsSkippedMaxSize() { return embedsSkippedMaxSize.get(); }
    public void incrementEmbeds() { embeds.incrementAndGet(); }
    public void incrementOcrSubmitted() { ocrSubmitted.incrementAndGet(); }
    public void incrementOcrCompleted() { ocrCompleted.incrementAndGet(); }
    public void incrementEmbedsSkippedMaxDepth() { embedsSkippedMaxDepth.incrementAndGet(); }
    public void incrementEmbedsSkippedMaxSize() { embedsSkippedMaxSize.incrementAndGet(); }
    /** A "unit" is the container's cheaply-countable top-level element (a PST message or an
     * archive entry). Used only to project a completion percentage; -1 means unknown. */
    public boolean setExpectedUnits(final long total) { return expectedUnits.compareAndSet(-1L, total); }
    public long expectedUnits() { return expectedUnits.get(); }
    public void incrementUnits() { unitsParsed.incrementAndGet(); }
    public long unitsParsed() { return unitsParsed.get(); }
    /** Snap the numerator to the known total, so a container whose parse has completed reads 100%
     * even when some sub-units were skipped, failed, or filtered. No-op when the total is unknown. */
    public void completeUnits() {
        final long total = expectedUnits.get();
        if (total >= 0) {
            unitsParsed.set(total);
        }
    }
    /** Marked by a parser that supplies its own unit numerator (PST), so EmbedSpawner does not also count. */
    public void markParserTracksUnits() { parserTracksUnits = true; }
    public boolean parserTracksUnits() { return parserTracksUnits; }
    public long elapsedMillis(final long now) { return now - startMillis; }
}
