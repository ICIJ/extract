package org.icij.extract.redis;

import org.icij.extract.extractor.ExtractionStatus;
import org.icij.extract.report.Report;
import org.junit.After;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.fest.assertions.Assertions.assertThat;

public class RedisReportMapTest {
    private RedisReportMap reportMap = new RedisReportMap("test:report", "redis://redis:6379");

    @Test
    public void test_insert_success() {
        Path key = Paths.get("/my/path");

        assertThat(reportMap.fastPut(key, new Report(ExtractionStatus.SUCCESS))).isTrue();

        assertThat(reportMap.get(key).getStatus()).isEqualTo(ExtractionStatus.SUCCESS);
        assertThat(reportMap.get(key).getException().isPresent()).isFalse();
    }

    @Test
    public void test_insert_error() {
        Path key = Paths.get("/my/path");

        assertThat(reportMap.fastPut(key, new Report(ExtractionStatus.FAILURE_NOT_PARSED, new RuntimeException("an error occurred")))).isTrue();

        assertThat(reportMap.get(key).getStatus()).isEqualTo(ExtractionStatus.FAILURE_NOT_PARSED);
        assertThat(reportMap.get(key).getException().get().getClass()).isEqualTo(RuntimeException.class);
        assertThat(reportMap.get(key).getException().get().getMessage()).isEqualTo("an error occurred");
    }

    @After
    public void tearDown() {
        reportMap.delete();
    }
}
