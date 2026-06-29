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
}
