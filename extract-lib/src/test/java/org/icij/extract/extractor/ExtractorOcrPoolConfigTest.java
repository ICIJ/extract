package org.icij.extract.extractor;

import org.icij.task.Options;
import org.junit.Test;
import java.util.Map;
import static org.fest.assertions.Assertions.assertThat;

public class ExtractorOcrPoolConfigTest {
    @Test public void testDefaultsToCpuCountAndFanoutOn() {
        Extractor extractor = new Extractor();
        assertThat(extractor.getOcrParallelism()).isEqualTo(Runtime.getRuntime().availableProcessors());
        assertThat(extractor.isOcrFanout()).isTrue();
        assertThat(extractor.getOcrMinImageBytes()).isEqualTo(0L);
        extractor.close();
    }

    @Test public void testOptionsOverrideValues() {
        Extractor extractor = new Extractor(Options.from(Map.of(
            "ocrParallelism", "3", "ocrFanout", "false", "ocrMinImageBytes", "4096")));
        assertThat(extractor.getOcrParallelism()).isEqualTo(3);
        assertThat(extractor.isOcrFanout()).isFalse();
        assertThat(extractor.getOcrMinImageBytes()).isEqualTo(4096L);
        extractor.close();
    }

    // Fix wave 7: the OCR pool must be null before any extraction occurs (lazy creation).
    // An Extractor that is only constructed — never used to extract — must hold no extra
    // threads and must be safe to close without any prior extraction.
    @Test public void testOcrPoolIsNullBeforeAnyExtraction() {
        Extractor extractor = new Extractor();
        // Pool must not be created at construction time.
        assertThat(extractor.ocrExecutorOrNull()).isNull();
        extractor.close();
    }

    @Test public void testOcrPoolIsNullBeforeAnyExtractionWithOptions() {
        Extractor extractor = new Extractor(Options.from(Map.of("ocrParallelism", "2")));
        assertThat(extractor.ocrExecutorOrNull()).isNull();
        extractor.close();
    }

    // close() with no prior extraction must be safe (null pool, scheduler never started).
    @Test public void testCloseWithNoExtractionIsIdempotentAndSafe() {
        Extractor extractor = new Extractor();
        // First close: nothing was created, both null-guards must fire without exception.
        extractor.close();
        // Second close: must be safe to call a second time (idempotent).
        extractor.close();
    }
}
