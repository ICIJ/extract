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
}
