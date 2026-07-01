package org.icij.extract.extractor;

import org.icij.task.Options;
import org.junit.Test;

import java.util.Map;

import static org.fest.assertions.Assertions.assertThat;

public class LegacyUntitledNamingOptionTest {

    @Test
    public void testDefaultsFalse() throws Exception {
        try (Extractor extractor = new Extractor(Options.from(Map.of()))) {
            assertThat(extractor.isLegacyUntitledNaming()).isFalse();
        }
    }

    @Test
    public void testOverrideTrue() throws Exception {
        try (Extractor extractor = new Extractor(Options.from(Map.of("legacyUntitledNaming", "true")))) {
            assertThat(extractor.isLegacyUntitledNaming()).isTrue();
        }
    }

    @Test
    public void testLegacyNamingForcesFanoutOff() throws Exception {
        // Legacy naming and PST folder fan-out are mutually exclusive: the global untitled_N counter is
        // serial-only, so enabling legacy naming must force pstFolderFanout=false even when explicitly on.
        try (Extractor extractor = new Extractor(Options.from(Map.of(
                "legacyUntitledNaming", "true", "pstFolderFanout", "true")))) {
            assertThat(extractor.isLegacyUntitledNaming()).isTrue();
            assertThat(extractor.isPstFolderFanout()).isFalse();
        }
    }
}
