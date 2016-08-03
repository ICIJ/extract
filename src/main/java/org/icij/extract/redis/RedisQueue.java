package org.icij.extract.redis;

import org.icij.extract.core.Queue;

import java.io.IOException;

import java.nio.file.Path;

import org.redisson.Config;
import org.redisson.RedissonBlockingQueue;
import org.redisson.command.CommandSyncService;
import org.redisson.connection.ConnectionManager;

/**
 * A {@link Queue} using Redis as a backend.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public class RedisQueue extends RedissonBlockingQueue<Path> implements Queue {

	/**
	 * The default name for a queue in Redis.
	 */
	public static final String DEFAULT_NAME = "extract:queue";

	private final ConnectionManager connectionManager;

	/**
	 * Create a Redis-backed queue using the provided configuration.
	 *
	 * @param config configuration for connecting to Redis
	 * @param name the name of the queue
	 * @return a new queue instance
	 */
	public static RedisQueue create(final Object config, final String name) {
		return new RedisQueue(ConnectionManagerFactory.createConnectionManager(config), name);
	}

	/**
	 * Create a Redis-backed queue using the default configuration, assuming Redis runs on localhost and uses the
	 * default port.
	 *
	 * @param name the name of the queue
	 * @return a new queue instance
	 */
	public static RedisQueue create(final String name) {
		return create(new Config().useSingleServer().setAddress("127.0.0.1:6379"), name);
	}

	/**
	 * Create a Redis-backed queue using the default configuration and name.
	 *
	 * @return a new queue instance
	 */
	public static RedisQueue create() {
		return create(null);
	}

	/**
	 * Instantiate a new Redis-backed queue using the provided connection manager and name.
	 *
	 * @param connectionManager instantiated using {@link ConnectionManagerFactory}
	 * @param name the name of the queue
	 */
	public RedisQueue(final ConnectionManager connectionManager, final String name) {
		super(new RedisQueueCodec(), new CommandSyncService(connectionManager),
			null == name || name.trim().isEmpty() ? DEFAULT_NAME : name);
		this.connectionManager = connectionManager;
	}

	@Override
	public void close() throws IOException {
		connectionManager.shutdown();
	}
}
