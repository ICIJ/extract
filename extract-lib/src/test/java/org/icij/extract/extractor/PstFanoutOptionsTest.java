package org.icij.extract.extractor;

import org.icij.task.Options;
import org.junit.Test;

import java.util.Map;

import static org.fest.assertions.Assertions.assertThat;

public class PstFanoutOptionsTest {

    @Test
    public void testDefaults() throws Exception {
        try (Extractor extractor = new Extractor(Options.from(Map.of()))) {
            assertThat(extractor.isPstFolderFanout()).isTrue();
            assertThat(extractor.getPstParseParallelism())
                    .isEqualTo(Runtime.getRuntime().availableProcessors());
            // No parse pool is created until first use.
            assertThat(extractor.parseExecutorOrNull()).isNull();
        }
    }

    @Test
    public void testOverridesAndLazyPoolClosedCleanly() throws Exception {
        try (Extractor extractor = new Extractor(Options.from(Map.of(
                "pstFolderFanout", "false",
                "pstParseParallelism", "4")))) {
            assertThat(extractor.isPstFolderFanout()).isFalse();
            assertThat(extractor.getPstParseParallelism()).isEqualTo(4);
            // Force lazy creation, then confirm close() tears it down without throwing.
            assertThat(extractor.parseExecutor()).isNotNull();
            assertThat(extractor.parseExecutorOrNull()).isNotNull();
        }
        // try-with-resources close() must not throw.
    }
}
