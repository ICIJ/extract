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

	public static String DEFAULT_NAME = "extract:queue";

	private final ConnectionManager connectionManager;

	public static RedisQueue create(final Object config, final String name) {
		return new RedisQueue(ConnectionManagerFactory.createConnectionManager(config), name);
	}

	public static RedisQueue create(final String name) {
		return create(new Config().useSingleServer().setAddress("127.0.0.1:6379"), name);
	}

	public RedisQueue(final ConnectionManager connectionManager, final String name) {
		super(new RedisQueueCodec(), new CommandSyncService(connectionManager),
			null == name ? DEFAULT_NAME : name);
		this.connectionManager = connectionManager;
	}

	@Override
	public void close() throws IOException {
		connectionManager.shutdown();
	}
}
