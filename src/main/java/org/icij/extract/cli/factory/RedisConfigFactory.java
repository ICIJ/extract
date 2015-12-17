package org.icij.extract.cli.factory;

import org.apache.commons.cli.CommandLine;

import org.redisson.Config;

/**
 * Factory methods for creating Redis configuration.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public class RedisConfigFactory {

	public static Object createConfig(final CommandLine cmd) {
		final Config config = new Config();
		final Object serverConfig;

		// TODO: support all the other types supported by the ConnectionManagerFactory.
		serverConfig = config.useSingleServer().setAddress(cmd.getOptionValue("redis-address", "127.0.0.1:6379"));

		return serverConfig;
	}
}
