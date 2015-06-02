package org.icij.extract.cli;

import org.icij.extract.core.*;

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
		super(logger, new String[] {
			"v", "q", "n", "redis-address"
		});
	}

	protected Option createOption(String name) {
		switch (name) {

		case "n": return Option.builder("n")
			.desc("The name of the queue to wipe. Defaults to \"extract\".")
			.longOpt("name")
			.hasArg()
			.argName("name")
			.build();

		case "q": return Option.builder("q")
			.desc("Set the queue backend type. For now, the only valid value and the default is \"redis\".")
			.longOpt("queue")
			.hasArg()
			.argName("type")
			.build();

		case "redis-address": return Option.builder()
			.desc("Set the Redis backend address. Defaults to 127.0.0.1:6379.")
			.longOpt(name)
			.hasArg()
			.argName("address")
			.build();

		default:
			return super.createOption(name);
		}
	}

	public CommandLine parse(String[] args) throws ParseException, IllegalArgumentException {
		final CommandLine cmd = super.parse(args);

		final QueueType queueType = QueueType.parse(cmd.getOptionValue('q', "redis"));

		if (QueueType.REDIS != queueType) {
			throw new IllegalArgumentException("Invalid queue type: " + queueType + ".");
		}

		final Redisson redisson = getRedisson(cmd);
		final RQueue<String> queue = redisson.getQueue(cmd.getOptionValue('n', "extract") + ":queue");

		logger.info("Wiping queue.");
		queue.delete();
		redisson.shutdown();

		return cmd;
	}

	public void printHelp() {
		super.printHelp(Command.WIPE_QUEUE, "Wipe a queue. The name option is respected.");
	}
}
