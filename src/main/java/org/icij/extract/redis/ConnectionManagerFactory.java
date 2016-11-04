package org.icij.extract.redis;

import org.icij.task.DefaultOption;

import org.redisson.config.Config;
import org.redisson.config.SingleServerConfig;
import org.redisson.connection.ConnectionManager;
import org.redisson.connection.SingleConnectionManager;

/**
 * Factory methods for creating a Redis client.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
class ConnectionManagerFactory {

	private static String DEFAULT_ADDRESS = "127.0.0.1:6379";

	/**
	 * Create a new connection manager for a Redis server at the default address.
	 *
	 * @return a new connection manager
	 */
	static ConnectionManager createConnectionManager() {
		return createConnectionManager(DEFAULT_ADDRESS);
	}

	/**
	 * Create a new connection manager by query the given set of options.
	 *
	 * @param options options containing connection parameters
	 * @return a new connection manager
	 */
	static ConnectionManager createConnectionManager(final DefaultOption.Set options) {
		return createConnectionManager(options.get("redis-address").value().orElse(DEFAULT_ADDRESS));
	}

	/**
	 * Create a new connection manager for a single server using the supplied address.
	 *
	 * @param address the Redis server address
	 * @return a new connection manager
	 */
	private static ConnectionManager createConnectionManager(final String address) {

		// TODO: support all the other types supported by the ConnectionManagerFactory.
		return createConnectionManager(new Config().useSingleServer().setAddress(address).setTimeout(60000));
	}

	/**
	 * Create a new connection manager using the supplied configuration.
	 *
	 * @param configuration configuration for generating the connection manager
	 * @return a new connection manager
	 */
	private static ConnectionManager createConnectionManager(final SingleServerConfig configuration) {

		// TODO: Create a hash of config options so that only one manager is used per unique server. This should
		// improve contention.
		return new SingleConnectionManager(configuration, new Config());
	}
}
