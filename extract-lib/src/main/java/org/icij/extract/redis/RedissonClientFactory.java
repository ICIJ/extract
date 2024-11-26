package org.icij.extract.redis;

import org.icij.task.Options;
import org.icij.task.annotation.Option;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.redisson.config.SingleServerConfig;

import java.net.URI;

import static java.util.Optional.ofNullable;

/**
 * Factory for creating a Redis client.
 *
 *
 */
@Option(name = "redisAddress", description = "Set the Redis backend address. Defaults to 127.0.0.1:6379.", parameter
		= "address")
@Option(name = "redisPoolSize", description = "Set the Redis backend pool size per collection.", parameter
		= "poolSize")
@Option(name = "redisTimeout", description = "The client timeout for Redis operations.", parameter = "timeout")
public class RedissonClientFactory {

	private String address = null;
	private int timeout = -1;
	private int poolSize = 1;

	/**
	 * Create a new connection manager by query the given set of options.
	 *
	 * @param options options containing connection parameters
	 * @return a new connection manager
	 */
	public RedissonClientFactory withOptions(final Options<String> options) {
		withAddress(options.valueIfPresent("redisAddress").orElse(null));
		options.ifPresent("redisTimeout", o -> o.parse().asInteger()).ifPresent(this::withTimeout);
		options.ifPresent("redisPoolSize", o -> o.parse().asInteger()).ifPresent(this::withPoolSize);
		return this;
	}

	/**
	 * Sets the size of the pool for this client
	 * @param poolSize
	 * @return chainable factory
	 */
	private RedissonClientFactory withPoolSize(final int poolSize) {
		this.poolSize = poolSize;
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
		Config config = createConfig();
		updateConfig(config);
		return Redisson.create(config);
	}

	/**
	 * Create a new connection manager for a single server using the supplied address.
	 *
	 * @return a new connection manager
	 */
	public CloseableRedissonClient createCloseable() {
		Config config = createConfig();
		updateConfig(config);
		return new CloseableRedissonClient(config);
	}

	void updateConfig(Config config) {
		final String address = null == this.address ? "redis://127.0.0.1:6379" : this.address;
		URI url = URI.create(address);
		String[] userPass = ofNullable(url.getUserInfo()).orElse("").split(":");
		final int timeout = this.timeout < 0 ? 60 * 1000 : this.timeout;
		SingleServerConfig singleServerConfig = config.useSingleServer();
		singleServerConfig.
				setConnectionPoolSize(poolSize).
				setConnectionMinimumIdleSize(poolSize).
				setAddress(address).
				setTimeout(timeout);
		if (userPass.length == 2) {
			singleServerConfig.setPassword(userPass[1]);
		}
	}

	Config createConfig() {
		return new Config();
	}
}
