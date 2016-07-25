package org.icij.extract.cli.factory;

import org.icij.extract.core.Queue;
import org.icij.extract.core.QueueType;
import org.icij.extract.core.ArrayQueue;
import org.icij.extract.redis.RedisQueue;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;

/**
 * Factory methods for creating queue objects.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public class QueueFactory {

	/**
	 * Creates {@code Queue} based on the given commandline arguments, preferring an in-local-memory queue by default.
	 *
	 * @param cmd the commandline argument object
	 * @return a {@code Queue} or {@code null}
	 * @throws ParseException if the commandline arguments could not be parsed
	 */
	public static Queue createQueue(final CommandLine cmd) throws ParseException {
		final QueueType queueType = QueueType.parse(cmd.getOptionValue('q', "array"));
		final Queue queue;
		final int buffer;

		if (cmd.hasOption("queue-buffer")) {
			buffer = ((Number) cmd.getParsedOptionValue("buffer-size")).intValue();
		} else {
			buffer = 1024;
		}

		if (QueueType.ARRAY == queueType) {
			queue = ArrayQueue.create(buffer);
		} else {
			queue = createSharedQueue(cmd);
		}

		return queue;
	}

	/**
	 * Creates a share {@code Queue} based on the given commandline arguments, preferring Redis by default.
	 *
	 * @param cmd the commandline argument object
	 * @return a {@code Queue} or {@code null}
	 * @throws ParseException if the commandline arguments could not be parsed
	 */
	public static Queue createSharedQueue(final CommandLine cmd) throws ParseException {
		final QueueType queueType = QueueType.parse(cmd.getOptionValue('q', "redis"));
		final String name = cmd.getOptionValue('n');
		final Queue queue;

		if (QueueType.REDIS == queueType) {
			queue = RedisQueue.create(RedisConfigFactory.createConfig(cmd), name);
		} else {
			queue = null;
		}

		return queue;
	}
}
