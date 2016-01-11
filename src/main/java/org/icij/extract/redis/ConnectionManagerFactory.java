package org.icij.extract.redis;

import org.redisson.Config;
import org.redisson.SingleServerConfig;
import org.redisson.connection.ConnectionManager;
import org.redisson.connection.SingleConnectionManager;

/**
 * Factory methods for creating a Redis client.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public class ConnectionManagerFactory {

	/**
	 * Create a new connection manager using the supplied configuration.
	 *
	 * @param config configuration for generating the connection manager
	 * @return a new connection manager
	 */
	public static ConnectionManager createConnectionManager(final Object config) {
		final ConnectionManager connectionManager;

		if (config instanceof SingleServerConfig) {
			connectionManager = new SingleConnectionManager((SingleServerConfig) config, new Config());
		} else {
			throw new IllegalArgumentException("Server(s) address(es) not defined!");
		}

		return connectionManager;
	}
}
