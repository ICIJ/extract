package org.icij.extract.extractor;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/** Tracks in-flight extractions and drives a periodic heartbeat to registered listeners. */
public class ExtractionProgressTracker implements AutoCloseable {
    private final ConcurrentHashMap<Path, ExtractionProgress> inFlight = new ConcurrentHashMap<>();
    private final List<ProgressListener> listeners = new CopyOnWriteArrayList<>();
    private final Duration heartbeatInterval;
    private final LongSupplier clock;
    private ScheduledExecutorService scheduler;

    public ExtractionProgressTracker(final Duration heartbeatInterval) {
        this(heartbeatInterval, System::currentTimeMillis);
    }

    public ExtractionProgressTracker(final Duration heartbeatInterval, final LongSupplier clock) {
        this.heartbeatInterval = heartbeatInterval;
        this.clock = clock;
    }

    public ExtractionProgress begin(final Path path) {
        final ExtractionProgress progress = new ExtractionProgress(path, clock.getAsLong());
        inFlight.put(path, progress);
        return progress;
    }

    public void end(final Path path) { inFlight.remove(path); }

    public Collection<ExtractionProgress> inFlight() { return inFlight.values(); }

    public void addListener(final ProgressListener listener) { listeners.add(listener); }

    void tick() {
        final Collection<ExtractionProgress> snapshot = inFlight();
        for (final ProgressListener listener : listeners) {
            listener.onHeartbeat(snapshot);
        }
    }

    public synchronized void start() {
        if (heartbeatInterval == null || heartbeatInterval.isZero() || heartbeatInterval.isNegative() || scheduler != null) {
            return;
        }
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            final Thread thread = new Thread(r, "extract-progress-heartbeat");
            thread.setDaemon(true);
            return thread;
        });
        final long millis = heartbeatInterval.toMillis();
        scheduler.scheduleAtFixedRate(this::tick, millis, millis, TimeUnit.MILLISECONDS);
    }

    @Override
    public synchronized void close() {
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
    }
}
