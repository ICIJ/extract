package org.icij.extract.extractor;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.DoubleSupplier;

import static org.fest.assertions.Assertions.assertThat;

public class MemoryPressureGaugeTest {

    @Test
    public void testHighWhenRatioMeetsThreshold() {
        MemoryPressureGauge gauge = new MemoryPressureGauge(0.7, () -> 0.8, 0L);
        assertThat(gauge.getAsBoolean()).isTrue();
    }

    @Test
    public void testLowWhenRatioBelowThreshold() {
        MemoryPressureGauge gauge = new MemoryPressureGauge(0.7, () -> 0.5, 0L);
        assertThat(gauge.getAsBoolean()).isFalse();
    }

    @Test
    public void testDisabledWhenThresholdNotInOpenUnitInterval() {
        assertThat(new MemoryPressureGauge(0.0, () -> 1.0, 0L).getAsBoolean()).isFalse();
        assertThat(new MemoryPressureGauge(1.0, () -> 1.0, 0L).getAsBoolean()).isFalse();
        assertThat(new MemoryPressureGauge(-1.0, () -> 1.0, 0L).getAsBoolean()).isFalse();
    }

    @Test
    public void testSampleIsCachedWithinInterval() {
        AtomicInteger calls = new AtomicInteger();
        DoubleSupplier ratio = () -> { calls.incrementAndGet(); return 0.9; };
        // Huge interval => only the first call samples; the rest return the cached value.
        MemoryPressureGauge gauge = new MemoryPressureGauge(0.7, ratio, Long.MAX_VALUE);

        assertThat(gauge.getAsBoolean()).isTrue();
        assertThat(gauge.getAsBoolean()).isTrue();
        assertThat(gauge.getAsBoolean()).isTrue();
        assertThat(calls.get()).isEqualTo(1);
    }

    @Test
    public void testResamplesEveryCallWhenIntervalZero() {
        AtomicInteger calls = new AtomicInteger();
        // Ratio crosses the threshold on the third sample.
        DoubleSupplier ratio = () -> calls.incrementAndGet() >= 3 ? 0.9 : 0.1;
        MemoryPressureGauge gauge = new MemoryPressureGauge(0.7, ratio, 0L);

        assertThat(gauge.getAsBoolean()).isFalse();
        assertThat(gauge.getAsBoolean()).isFalse();
        assertThat(gauge.getAsBoolean()).isTrue();
    }
}
