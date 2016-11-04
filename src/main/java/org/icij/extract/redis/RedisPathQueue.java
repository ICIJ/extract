package org.icij.extract.redis;

import org.icij.extract.core.PathQueue;

import java.io.IOException;

import java.nio.file.Path;

import org.icij.task.DefaultOption;
import org.redisson.RedissonBlockingQueue;
import org.redisson.command.CommandSyncService;
import org.redisson.connection.ConnectionManager;

/**
 * A {@link PathQueue} using Redis as a backend.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public class RedisPathQueue extends RedissonBlockingQueue<Path> implements PathQueue {

	/**
	 * The default name for a queue in Redis.
	 */
	public static final String DEFAULT_NAME = "extract:queue";

	private final ConnectionManager connectionManager;

	/**
	 * Create a Redis-backed queue using the provided configuration.
	 *
	 * @param options options for connecting to Redis
	 * @param name the name of the queue
	 */
	public RedisPathQueue(final DefaultOption.Set options, final String name) {
		this(ConnectionManagerFactory.createConnectionManager(options), name);
	}

	/**
	 * Instantiate a Redis-backed queue using the default configuration, assuming Redis runs on localhost and uses the
	 * default port.
	 *
	 * @param name the name of the queue
	 */
	public RedisPathQueue(final String name) {
		this(ConnectionManagerFactory.createConnectionManager(), name);
	}

	/**
	 * Instantiate a Redis-backed queue using the default configuration and name.
	 */
	public RedisPathQueue() {
		this(ConnectionManagerFactory.createConnectionManager(), DEFAULT_NAME);
	}

	/**
	 * Instantiate a new Redis-backed queue using the provided connection manager and name.
	 *
	 * @param connectionManager instantiated using {@link ConnectionManagerFactory}
	 * @param name the name of the queue
	 */
	private RedisPathQueue(final ConnectionManager connectionManager, final String name) {
		super(new RedisPathQueueCodec(), new CommandSyncService(connectionManager),
			null == name || name.trim().isEmpty() ? DEFAULT_NAME : name);
		this.connectionManager = connectionManager;
	}

	@Override
	public void close() throws IOException {
		connectionManager.shutdown();
	}
}
