package org.icij.extract.redis;

import org.icij.task.Options;
import org.redisson.config.Config;
import org.redisson.connection.ConnectionManager;
import org.redisson.connection.SingleConnectionManager;

/**
 * Factory for creating a Redis client.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public class ConnectionManagerFactory {

	private String address = null;

	/**
	 * Create a new connection manager by query the given set of options.
	 *
	 * @param options options containing connection parameters
	 * @return a new connection manager
	 */
	public ConnectionManagerFactory withOptions(final Options<String> options) {
		return withAddress(options.get("redis-address").value().orElse(null));
	}

	private ConnectionManagerFactory withAddress(final String address) {
		this.address = address;
		return this;
	}

	/**
	 * Create a new connection manager for a single server using the supplied address.
	 *
	 * @return a new connection manager
	 */
	public ConnectionManager create() {
		final String address = null == this.address ? "127.0.0.1:6379" : this.address;

		// TODO: support all the other types supported by the ConnectionManagerFactory.
		// TODO: Create a hash of config options so that only one manager is used per unique server. This should
		// improve contention.
		return new SingleConnectionManager(new Config()
				.useSingleServer()
				.setAddress(address)
				.setTimeout(6000), new Config());
	}
}
