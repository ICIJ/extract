package org.icij.extract.redis;

import org.icij.extract.core.PathQueue;

import java.io.IOException;

import java.nio.file.Path;

import org.icij.task.Options;
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
	 */
	public RedisPathQueue(final Options<String> options) {
		this(new ConnectionManagerFactory().withOptions(options).create(),
				options.get("queue-name").value().orElse(DEFAULT_NAME));
	}

	/**
	 * Instantiate a new Redis-backed queue using the provided connection manager and name.
	 *
	 * @param connectionManager instantiated using {@link ConnectionManagerFactory}
	 * @param name the name of the queue
	 */
	private RedisPathQueue(final ConnectionManager connectionManager, final String name) {
		super(new RedisPathQueueCodec(), new CommandSyncService(connectionManager), null == name ? DEFAULT_NAME : name);
		this.connectionManager = connectionManager;
	}

	@Override
	public void close() throws IOException {
		connectionManager.shutdown();
	}
}
