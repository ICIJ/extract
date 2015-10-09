package org.icij.extract.cli;

import org.icij.extract.core.*;
import org.icij.extract.cli.options.*;
import org.icij.extract.redis.Redis;

import java.util.logging.Logger;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;

import org.redisson.Redisson;
import org.redisson.core.RQueue;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public class WipeQueueCli extends Cli {

	public WipeQueueCli(Logger logger) {
		super(logger, new QueueOptionSet(), new RedisOptionSet());
	}

	public CommandLine parse(String[] args) throws ParseException, IllegalArgumentException {
		final CommandLine cmd = super.parse(args);

		final QueueType queueType = QueueType.parse(cmd.getOptionValue('q', "redis"));

		if (QueueType.REDIS != queueType) {
			throw new IllegalArgumentException("Invalid queue type: " + queueType + ".");
		}

		final Redisson redisson = Redis.createClient(cmd.getOptionValue("redis-address"));
		final RQueue<String> queue = Redis.getQueue(redisson, cmd.getOptionValue("queue-name"));

		logger.info("Wiping queue.");
		queue.delete();
		redisson.shutdown();

		return cmd;
	}

	public void printHelp() {
		super.printHelp(Command.WIPE_QUEUE, "Wipe a queue. The name option is respected.");
	}
}
