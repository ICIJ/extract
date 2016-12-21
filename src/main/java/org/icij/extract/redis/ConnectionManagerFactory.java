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
	private int timeout = -1;

	/**
	 * Create a new connection manager by query the given set of options.
	 *
	 * @param options options containing connection parameters
	 * @return a new connection manager
	 */
	public ConnectionManagerFactory withOptions(final Options<String> options) {
		withAddress(options.get("redis-address").value().orElse(null));
		options.get("redis-timeout").parse().asInteger().ifPresent(this::withTimeout);
		return this;
	}

	/**
	 * Set the Redis server address. Uses {@literal 127.0.0.1:6379} by default.
	 *
	 * @param address the Redis server address
	 * @return chainable factory
	 */
	private ConnectionManagerFactory withAddress(final String address) {
		this.address = address;
		return this;
	}

	/**
	 * Set the connection timeout. Uses a 60-second timeout by default.
	 *
	 * @param timeout the timeout in milliseconds
	 * @return chainable factory
	 */
	private ConnectionManagerFactory withTimeout(final int timeout) {
		this.timeout = timeout;
		return this;
	}

	/**
	 * Create a new connection manager for a single server using the supplied address.
	 *
	 * @return a new connection manager
	 */
	public ConnectionManager create() {
		final String address = null == this.address ? "127.0.0.1:6379" : this.address;
		final int timeout = this.timeout < 0 ? 60 * 1000 : this.timeout;

		// TODO: support all the other types supported by the ConnectionManagerFactory.
		// TODO: Create a hash of config options so that only one manager is used per unique server. This should
		// improve contention.
		return new SingleConnectionManager(new Config()
				.useSingleServer()
				.setAddress(address)
				.setTimeout(timeout), new Config());
	}
}
