package org.icij.extract.redis;

import org.icij.extract.queue.DocumentSet;
import org.icij.task.Options;
import org.redisson.Redisson;
import org.redisson.RedissonSet;
import org.redisson.api.RedissonClient;
import org.redisson.command.CommandSyncService;
import org.redisson.liveobject.core.RedissonObjectBuilder;

import java.nio.charset.Charset;
import java.util.HashMap;

public class RedisDocumentSet<T> extends RedissonSet<T> implements DocumentSet<T> {
    public static String DEFAULT_NAME = "extract:filter";
	private final RedissonClient redissonClient;

	/**
   	 * Create a Redis-backed set for filtering queues
   	 *
   	 * @param setName name of the redis key
   	 * @param redisAddress redis url i.e. redis://127.0.0.1:6379
   	 */
   	public RedisDocumentSet(final String setName, final String redisAddress) {
   		this(Options.from(new HashMap<String, String>() {{
   			put("redisAddress", redisAddress);
   			put("setName", setName);
   		}}));
   	}

    public RedisDocumentSet(Options<String> options) {
        this(new RedissonClientFactory().withOptions(options).create(),
        				options.valueIfPresent("setName").orElse(DEFAULT_NAME),
        				Charset.forName(options.valueIfPresent("charset").orElse("UTF-8")));
    }

    private RedisDocumentSet(RedissonClient redissonClient, String name, Charset charset) {
        super(new RedisDocumentQueue.PathQueueCodec(charset),
        				new CommandSyncService(((Redisson)redissonClient).getConnectionManager(), new RedissonObjectBuilder(redissonClient)),
        				null == name ? DEFAULT_NAME : name, redissonClient);
        this.redissonClient = redissonClient;
    }

	@Override
	public String toString() {
		return "RedisDocumentSet{name=" + getName() + '}';
	}

	@Override
	public void close() {
		redissonClient.shutdown();
	}
}
