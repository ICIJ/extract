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

	public static Queue createQueue(final CommandLine cmd) throws ParseException {
		final QueueType queueType = QueueType.parse(cmd.getOptionValue('q', "array"));
		final String name = cmd.getOptionValue("queue-name");
		final Queue queue;
		final int buffer;

		if (cmd.hasOption("queue-buffer")) {
			buffer = ((Number) cmd.getParsedOptionValue("buffer-size")).intValue();
		} else{
			buffer = 1024;
		}

		if (QueueType.REDIS == queueType) {
			queue = RedisQueue.create(RedisConfigFactory.createConfig(cmd), name);
		} else if (QueueType.ARRAY == queueType) {
			queue = ArrayQueue.create(buffer);
		} else {
			queue = null;
		}

		return queue;
	}
}
