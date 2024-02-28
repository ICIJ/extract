package org.icij.extract.redis;

import org.icij.task.Options;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.redisson.config.Config;
import org.redisson.config.SingleServerConfig;

import java.util.HashMap;

import static org.mockito.Answers.RETURNS_DEFAULTS;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class RedissonClientFactoryTest {
    @Mock private Config config;
    private final SingleServerConfig singleServerConfig = mock(SingleServerConfig.class, new SelfReturningAnswer());
    @Test
    public void test_build_default_params() {
        new DummyRedissonClientFactory().updateConfig(config);
        verify(singleServerConfig).setAddress("redis://127.0.0.1:6379");
    }

    @Test
    public void test_build_with_address() {
        new DummyRedissonClientFactory().withOptions(Options.from(new HashMap<>() {{
            put("redisAddress", "redis");
        }})).updateConfig(config);
        verify(singleServerConfig).setAddress("redis");
    }

    @Test
    public void test_build_with_address_pass() {
        new DummyRedissonClientFactory().withOptions(Options.from(new HashMap<>() {{
            put("redisAddress", "rediss://user:pass@redis:25061");
        }})).updateConfig(config);
        verify(singleServerConfig).setAddress("rediss://user:pass@redis:25061");
        verify(singleServerConfig).setPassword("pass");
    }

    @Before
    public void setUp() {
        initMocks(this);
        when(config.useSingleServer()).thenReturn(singleServerConfig);
    }

    private class DummyRedissonClientFactory extends RedissonClientFactory {
        @Override Config createConfig() {
            return config;
        }
    }

    private static class SelfReturningAnswer implements Answer<Object> {
        public Object answer(InvocationOnMock invocation) throws Throwable {
            Object mock = invocation.getMock();
            if(invocation.getMethod().getReturnType().isInstance(mock)){
                return mock;
            }
            return RETURNS_DEFAULTS.answer(invocation);
        }
    }
}
