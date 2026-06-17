package org.icij.extract.extractor;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.util.Locale;
import java.util.function.BooleanSupplier;
import java.util.function.DoubleSupplier;

/**
 * Reports whether the JVM heap is under memory pressure, used to make embedded-text buffering spill
 * to disk early — before a fixed byte budget would — so extraction of very large containers (PSTs,
 * mailboxes) stays within whatever heap is available regardless of how big the document tree grows.
 *
 * <p>The signal is the post-GC usage ratio of the tenured/old-generation heap pool, which approximates
 * the live working set rather than transient garbage. Sampling is throttled (the value is cached for a
 * short interval) because the check sits on the per-write hot path. A threshold outside the open
 * interval (0, 1) disables the gauge entirely, falling back to pure byte-budget behaviour.
 */
class MemoryPressureGauge implements BooleanSupplier {

    private final double threshold;
    private final DoubleSupplier usageRatio;
    private final long sampleIntervalNanos;

    private boolean sampled = false;
    private long lastSampleNanos = 0L;
    private boolean cachedHigh = false;

    MemoryPressureGauge(final double threshold) {
        this(threshold, defaultRatioSupplier(), 100_000_000L); // sample at most every 100ms
    }

    /** Test seam: inject the usage ratio and sample interval. */
    MemoryPressureGauge(final double threshold, final DoubleSupplier usageRatio, final long sampleIntervalNanos) {
        this.threshold = threshold;
        this.usageRatio = usageRatio;
        this.sampleIntervalNanos = sampleIntervalNanos;
    }

    @Override
    public boolean getAsBoolean() {
        // A threshold outside (0, 1) disables adaptive spilling.
        if (!(threshold > 0.0 && threshold < 1.0)) {
            return false;
        }
        final long now = System.nanoTime();
        if (!sampled || now - lastSampleNanos >= sampleIntervalNanos) {
            cachedHigh = usageRatio.getAsDouble() >= threshold;
            lastSampleNanos = now;
            sampled = true;
        }
        return cachedHigh;
    }

    private static DoubleSupplier defaultRatioSupplier() {
        final MemoryPoolMXBean pool = findHeapPool();
        return () -> {
            if (null == pool) {
                return 0.0;
            }
            // Prefer collection usage (state right after the last GC of this pool): it reflects the
            // live set rather than not-yet-collected garbage, so we don't spill on collectable bytes.
            MemoryUsage usage = pool.getCollectionUsage();
            if (null == usage || usage.getMax() <= 0) {
                usage = pool.getUsage();
            }
            if (null == usage || usage.getMax() <= 0) {
                return 0.0;
            }
            return (double) usage.getUsed() / (double) usage.getMax();
        };
    }

    /**
     * The tenured/old-generation heap pool for generational collectors, or the largest heap pool for
     * region/single-space collectors (G1 old gen, ZGC's ZHeap, Shenandoah). Null if none is exposed.
     */
    private static MemoryPoolMXBean findHeapPool() {
        MemoryPoolMXBean fallback = null;
        long largestMax = -1;
        for (final MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
            if (pool.getType() != MemoryType.HEAP) {
                continue;
            }
            final String name = pool.getName().toLowerCase(Locale.ROOT);
            if (name.contains("old") || name.contains("tenured")) {
                return pool;
            }
            final MemoryUsage usage = pool.getUsage();
            final long max = null != usage ? usage.getMax() : -1;
            if (max > largestMax) {
                largestMax = max;
                fallback = pool;
            }
        }
        return fallback;
    }
}
