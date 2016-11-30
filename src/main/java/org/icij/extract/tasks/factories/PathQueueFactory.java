package org.icij.extract.tasks.factories;

import org.icij.extract.core.PathQueue;
import org.icij.extract.PathQueueType;
import org.icij.extract.core.ArrayPathQueue;
import org.icij.extract.redis.RedisPathQueue;
import org.icij.task.Options;

/**
 * Factory methods for creating queue objects.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public class PathQueueFactory {

	private PathQueueType type = null;
	private Options<String> options = null;

	public PathQueueFactory(final Options<String> options) {
		type = options.get("queue-type").parse().asEnum(PathQueueType::parse).orElse(PathQueueType.ARRAY);
		this.options = options;
	}

	/**
	 * Creates {@code Queue} based on the given arguments, preferring an in-local-memory queue by default.
	 *
	 * @return a {@code Queue} or {@code null}
	 * @throws IllegalArgumentException if the arguments do not contain a valid queue type
	 */
	public PathQueue create() throws IllegalArgumentException {
		if (PathQueueType.ARRAY == type) {
			return new ArrayPathQueue(options.get("queue-buffer").parse().asInteger().orElse(1024));
		}

		return createShared();
	}

	/**
	 * Creates a share {@code Queue} based on the given commandline arguments, preferring Redis by default.
	 *
	 * @return a {@code Queue} or {@code null}
	 * @throws IllegalArgumentException if the given options do not contain a valid shared queue type
	 */
	public PathQueue createShared() throws IllegalArgumentException {
		if (PathQueueType.REDIS == type) {
			return new RedisPathQueue(options);
		}

		throw new IllegalArgumentException(String.format("\"%s\" is not a valid shared queue type.", type));
	}
}
