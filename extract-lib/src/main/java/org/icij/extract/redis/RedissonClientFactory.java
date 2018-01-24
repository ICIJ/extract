package org.icij.extract.redis;

import org.icij.task.Options;
import org.icij.task.annotation.Option;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

/**
 * Factory for creating a Redis client.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
@Option(name = "redisAddress", description = "Set the Redis backend address. Defaults to 127.0.0.1:6379.", parameter
		= "address")
@Option(name = "redisTimeout", description = "The client timeout for Redis operations.", parameter = "timeout")
public class RedissonClientFactory {

	private String address = null;
	private int timeout = -1;

	/**
	 * Create a new connection manager by query the given set of options.
	 *
	 * @param options options containing connection parameters
	 * @return a new connection manager
	 */
	public RedissonClientFactory withOptions(final Options<String> options) {
		withAddress(options.valueIfPresent("redisAddress").orElse(null));
		options.ifPresent("redisTimeout", o -> o.parse().asInteger()).ifPresent(this::withTimeout);
		return this;
	}

	/**
	 * Set the Redis server address. Uses {@literal 127.0.0.1:6379} by default.
	 *
	 * @param address the Redis server address
	 * @return chainable factory
	 */
	private RedissonClientFactory withAddress(final String address) {
		this.address = address;
		return this;
	}

	/**
	 * Set the connection timeout. Uses a 60-second timeout by default.
	 *
	 * @param timeout the timeout in milliseconds
	 * @return chainable factory
	 */
	private RedissonClientFactory withTimeout(final int timeout) {
		this.timeout = timeout;
		return this;
	}

	/**
	 * Create a new connection manager for a single server using the supplied address.
	 *
	 * @return a new connection manager
	 */
	public RedissonClient create() {
		final String address = null == this.address ? "redis://127.0.0.1:6379" : this.address;
		final int timeout = this.timeout < 0 ? 60 * 1000 : this.timeout;

		// TODO: support all the other types supported by the RedissonClientFactory.
		// TODO: Create a hash of config options so that only one manager is used per unique server. This should
		// improve contention.
		Config config = new Config();
		config.useSingleServer().setAddress(address).setTimeout(timeout);
        return Redisson.create(config);
	}
}
