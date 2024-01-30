package org.icij.extract.redis;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.icij.extract.extractor.ExtractionStatus;
import org.icij.extract.report.Report;
import org.icij.task.Options;
import org.junit.After;
import org.junit.Test;
import org.redisson.Redisson;
import org.redisson.RedissonMap;
import org.redisson.RedissonShutdownException;
import org.redisson.api.RedissonClient;
import org.redisson.client.protocol.Encoder;
import org.redisson.command.CommandSyncService;
import org.redisson.liveobject.core.RedissonObjectBuilder;

import java.io.IOException;
import java.io.StreamCorruptedException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;

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

    @Test
    public void test_insert_error_with_error_in_object_serialization() {
        Options<String> from = Options.from(new HashMap<String, String>() {{
            put("redisAddress", "redis://redis:6379");
        }});
        RedissonClient redissonClient = new RedissonClientFactory().withOptions(from).create();
        RedissonMap<Path, Report> badReportMap = new RedissonMap<>(new RedisReportMap.ReportCodec(Charset.defaultCharset()) {
            @Override
            public Encoder getMapValueEncoder() {
                return new ResultEncoder() {
                    @Override
                    public ByteBuf encode(Object in) {
                        ByteBuf out = ByteBufAllocator.DEFAULT.buffer();
                        out.writeBytes("4|this is not deserializable by decoder".getBytes());
                        return out;
                    }
                };
            }
        }, new CommandSyncService(((Redisson) redissonClient).getConnectionManager(), new RedissonObjectBuilder(redissonClient)), "test:report", redissonClient, null, null);

        Path key = Paths.get("/my/path");
        assertThat(badReportMap.fastPut(key, new Report(ExtractionStatus.FAILURE_NOT_PARSED, new RuntimeException("an error occurred")))).isTrue();

        assertThat(badReportMap.get(key).getStatus()).isEqualTo(ExtractionStatus.FAILURE_NOT_PARSED);
        assertThat(badReportMap.get(key).getException().get().getClass()).isEqualTo(StreamCorruptedException.class);
    }

    @Test
    public void test_insert_override_previous_value() {
        Path key = Paths.get("/my/path");

        assertThat(reportMap.fastPut(key, new Report(ExtractionStatus.FAILURE_NOT_PARSED, new RuntimeException("an error occurred")))).isTrue();
        reportMap.putAll(new HashMap<Path, Report>() {{put(key, new Report(ExtractionStatus.SUCCESS));}});

        assertThat(reportMap.get(key).getStatus()).isEqualTo(ExtractionStatus.SUCCESS);
    }

    @Test
    public void test_delete_success() {
        Path key = Paths.get("/my/path");
        reportMap.fastPut(key, new Report(ExtractionStatus.SUCCESS));
        assertThat(reportMap.size()).isEqualTo(1);
        assertThat(reportMap.delete()).isTrue();
        assertThat(reportMap.size()).isEqualTo(0);
    }

    @Test(expected = RedissonShutdownException.class)
    public void test_close_should_shutdown_redis_if_created() throws IOException {
        RedisReportMap reportMap = new RedisReportMap("test:report", "redis://redis:6379");
        reportMap.close();
        reportMap.fastPut(Paths.get("path"), new Report(ExtractionStatus.SUCCESS));
    }

    @Test
    public void test_close_should_not_shutdown_redis_if_not_created() throws IOException {
        RedissonClient redissonClient = new RedissonClientFactory().withOptions(Options.from(new HashMap<>() {{
            put("redisAddress", "redis://redis:6379");
        }})).create();
        try (RedisDocumentSet<String> ignored = new RedisDocumentSet<>(redissonClient, "test:report", Charset.defaultCharset(), String.class)) {}
        assertThat(redissonClient.isShutdown()).isFalse();
        redissonClient.shutdown();
    }

    @After
    public void tearDown() {
        reportMap.delete();
    }

    private static class TestingRedisReportMap extends RedisReportMap {
        public TestingRedisReportMap(RedissonClient redissonClient, String name, Charset charset) {
            super(redissonClient, name, charset);
        }
    }
}
