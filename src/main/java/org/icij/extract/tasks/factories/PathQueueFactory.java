package org.icij.extract.tasks.factories;

import org.icij.extract.core.PathQueue;
import org.icij.extract.PathQueueType;
import org.icij.extract.core.ArrayPathQueue;
import org.icij.extract.redis.RedisPathQueue;

import org.icij.task.DefaultOption;

import java.util.Optional;

/**
 * Factory methods for creating queue objects.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public class PathQueueFactory {

	/**
	 * Creates {@code Queue} based on the given commandline arguments, preferring an in-local-memory queue by default.
	 *
	 * @param options the options for creating the queue
	 * @return a {@code Queue} or {@code null}
	 * @throws IllegalArgumentException if the the commandline arguments do not contain a valid queue type
	 */
	public static PathQueue createQueue(final DefaultOption.Set options) throws IllegalArgumentException {
		final PathQueueType queueType = options.get("queue-type").asEnum(PathQueueType::parse).orElse(PathQueueType.ARRAY);

		if (PathQueueType.ARRAY == queueType) {
			return ArrayPathQueue.create(options.get("queue-buffer").asInteger().orElse(1024));
		}

		return createSharedQueue(options);
	}

	/**
	 * Creates a share {@code Queue} based on the given commandline arguments, preferring Redis by default.
	 *
	 * @param options the options for creating the queue
	 * @return a {@code Queue} or {@code null}
	 * @throws IllegalArgumentException if the given options do not contain a valid shared queue type
	 */
	public static PathQueue createSharedQueue(final DefaultOption.Set options) throws IllegalArgumentException {
		final PathQueueType queueType = options.get("queue-type").asEnum(PathQueueType::parse).orElse(PathQueueType.REDIS);
		final Optional<String> name = options.get("queue-name").value();

		if (PathQueueType.REDIS == queueType) {
			return new RedisPathQueue(options, name.orElse(RedisPathQueue.DEFAULT_NAME));
		}

		throw new IllegalArgumentException(String.format("\"%s\" is not a valid shared queue type.", queueType));
	}
}
