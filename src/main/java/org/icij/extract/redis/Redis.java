package org.icij.extract.redis;

import org.redisson.Config;
import org.redisson.Redisson;
import org.redisson.core.RQueue;
import org.redisson.core.RBlockingQueue;
import org.redisson.core.RMap;
import org.redisson.client.codec.StringCodec;

/**
 * Factory methods for use with Redis.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public class Redis {

	public static String DEFAULT_NAME = "extract";

	public static Redisson createClient(final String address) {
		final Config config;

		// Nothing to configure. Use default options.
		if (null == address) {
			return Redisson.create();
		}

		config = new Config();

		// TODO: Create a cluster if more than one address is given.
		config.useSingleServer().setAddress(address);

		return Redisson.create(config);
	}

	public static RQueue<String> getQueue(final Redisson client, String name) {
		return client.getQueue((null != name ? name : DEFAULT_NAME) + ":queue", new StringCodec());
	}

	public static RBlockingQueue<String> getBlockingQueue(final Redisson client, String name) {
		return client.getBlockingQueue((null != name ? name : DEFAULT_NAME) + ":queue", new StringCodec());
	}

	public static RMap<String, Integer> getReport(final Redisson client, String name) {
		return client.getMap((null != name ? name : DEFAULT_NAME) + ":report", new StringLongMapCodec());
	}
}
