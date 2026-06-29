package org.icij.extract.extractor;

import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

public class EmbedSpawnerUntitledTest {

    @Test
    public void testUntitledNameIsDeterministicPerParentAndOrdinal() {
        // Same parent + same sibling ordinal -> same name, regardless of global walk order.
        assertThat(EmbedSpawner.untitledName("parentA", 0))
                .isEqualTo(EmbedSpawner.untitledName("parentA", 0));
        // Different ordinal under the same parent -> different name.
        assertThat(EmbedSpawner.untitledName("parentA", 0))
                .isNotEqualTo(EmbedSpawner.untitledName("parentA", 1));
        // Different parent -> different name (avoids collisions across parents).
        assertThat(EmbedSpawner.untitledName("parentA", 0))
                .isNotEqualTo(EmbedSpawner.untitledName("parentB", 0));
    }
}
