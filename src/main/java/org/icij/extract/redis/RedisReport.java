package org.icij.extract.redis;

import org.icij.extract.core.Report;
import org.icij.extract.core.ExtractionResult;

import java.io.IOException;

import java.nio.file.Path;

import org.redisson.Config;
import org.redisson.RedissonMap;
import org.redisson.command.CommandSyncService;
import org.redisson.connection.ConnectionManager;

/**
 * A {@link Report} using Redis as a backend.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public class RedisReport extends RedissonMap<Path, ExtractionResult> implements Report {

	/**
	 * The default name for a report in Redis.
	 */
	public static final String DEFAULT_NAME = "extract:report";

	private final ConnectionManager connectionManager;

	/**
	 * Create a Redis-backed report using the provided configuration.
	 *
	 * @param config configuration for connecting to Redis
	 * @param name the name of the report
	 * @return a new report instance
	 */
	public static RedisReport create(final Object config, final String name) {
		return new RedisReport(ConnectionManagerFactory.createConnectionManager(config), name);
	}

	/**
	 * Create a Redis-backed report using the default configuration, assuming Redis runs on localhost and uses the
	 * default port.
	 *
	 * @param name the name of the report
	 * @return a new report instance
	 */
	public static RedisReport create(final String name) {
		return create(new Config().useSingleServer().setAddress("127.0.0.1:6379"), name);
	}

	/**
	 * Instantiate a new Redis-backed report using the provided connection manager and name.
	 *
	 * @param connectionManager instantiated using {@link ConnectionManagerFactory}
	 * @param name the name of the report
	 */
	public RedisReport(final ConnectionManager connectionManager, final String name) {
		super(new RedisReportCodec(), new CommandSyncService(connectionManager),
			null == name ? DEFAULT_NAME : name);
		this.connectionManager = connectionManager;
	}

	@Override
	public void close() throws IOException {
		connectionManager.shutdown();
	}
}
