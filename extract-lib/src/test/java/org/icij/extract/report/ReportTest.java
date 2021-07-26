package org.icij.extract.report;

import org.icij.extract.extractor.ExtractionStatus;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;


public class ReportTest {
    @Test
    public void test_equals_with_exception() {
        assertThat(new Report(ExtractionStatus.FAILURE_UNKNOWN, new RuntimeException("test"))).
                isEqualTo(new Report(ExtractionStatus.FAILURE_UNKNOWN, new RuntimeException("test")));
    }

    @Test
    public void test_equals_with_success() {
        assertThat(new Report(ExtractionStatus.SUCCESS)).isEqualTo(new Report(ExtractionStatus.SUCCESS));
    }
}
