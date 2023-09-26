package org.icij.extract.report;

import org.icij.extract.extractor.ExtractionStatus;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.fest.assertions.Assertions.assertThat;

public class HashMapReportMapTest {
    private HashMapReportMap reportMap = new HashMapReportMap();

    @Test
    public void test_insert_success() {
        Path key = Paths.get("/my/path");
        reportMap.fastPut(key, new Report(ExtractionStatus.SUCCESS));
        assertThat(reportMap.get(key).getStatus()).isEqualTo(ExtractionStatus.SUCCESS);
        assertThat(reportMap.get(key).getException().isPresent()).isFalse();
    }

    @Test
    public void test_correct_size() {
        reportMap.fastPut(Paths.get("/my/path/foo"), new Report(ExtractionStatus.SUCCESS));
        reportMap.fastPut(Paths.get("/my/path/bar"), new Report(ExtractionStatus.FAILURE_UNKNOWN));
        assertThat(reportMap.size()).isEqualTo(2);
    }

    @Test
    public void test_delete() {
        reportMap.fastPut(Paths.get("/my/path/foo"), new Report(ExtractionStatus.SUCCESS));
        reportMap.fastPut(Paths.get("/my/path/bar"), new Report(ExtractionStatus.FAILURE_UNKNOWN));
        assertThat(reportMap.delete()).isTrue();
        assertThat(reportMap.size()).isEqualTo(0);
    }
}
