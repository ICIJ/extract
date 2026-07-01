package org.icij.extract.parser;

import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

public class PstStdoutFilterFanoutTest {

    @Test
    public void testSuppressionArmedInsideFolderTaskBody() {
        PstStdoutFilter.install();
        // Simulate a pool thread with no suppression armed at entry.
        assertThat(PstStdoutFilter.depthForTest()).isEqualTo(0);
        PstStdoutFilter.runWithSuppression(() ->
                assertThat(PstStdoutFilter.depthForTest()).isGreaterThan(0));
        // Balanced: depth returns to 0 after the task body.
        assertThat(PstStdoutFilter.depthForTest()).isEqualTo(0);
    }
}
