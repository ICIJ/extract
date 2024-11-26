package org.icij.extract.redis;

import org.redisson.Redisson;
import org.redisson.config.Config;

import java.io.Closeable;
import java.io.IOException;

public class CloseableRedissonClient extends Redisson implements Closeable {

    protected CloseableRedissonClient(Config config) {
        super(config);
    }

    @Override
    public void close() throws IOException {
        shutdown();
    }
}
