package org.icij.extract.extractor;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

/** Mutable, thread-safe per-file extraction progress snapshot. */
public class ExtractionProgress {
    private final Path path;
    private final long startMillis;
    private final AtomicLong embeds = new AtomicLong();
    private final AtomicLong ocrSubmitted = new AtomicLong();
    private final AtomicLong ocrCompleted = new AtomicLong();

    public ExtractionProgress(final Path path, final long startMillis) {
        this.path = path;
        this.startMillis = startMillis;
    }

    public Path path() { return path; }
    public long startMillis() { return startMillis; }
    public long embedsParsed() { return embeds.get(); }
    public long ocrSubmitted() { return ocrSubmitted.get(); }
    public long ocrCompleted() { return ocrCompleted.get(); }
    public void incrementEmbeds() { embeds.incrementAndGet(); }
    public void incrementOcrSubmitted() { ocrSubmitted.incrementAndGet(); }
    public void incrementOcrCompleted() { ocrCompleted.incrementAndGet(); }
    public long elapsedMillis(final long now) { return now - startMillis; }
}
