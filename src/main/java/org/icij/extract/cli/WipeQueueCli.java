package org.icij.extract.cli;

import org.icij.extract.core.Queue;
import org.icij.extract.cli.options.QueueOptionSet;
import org.icij.extract.cli.options.RedisOptionSet;
import org.icij.extract.cli.factory.QueueFactory;

import java.io.IOException;

import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;

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

	public CommandLine parse(final String[] args) throws ParseException, IllegalArgumentException {
		final CommandLine cmd = super.parse(args);

		final Queue queue = QueueFactory.createQueue(cmd);

		logger.info("Wiping queue.");
		queue.clear();

		try {
			queue.close();
		} catch (IOException e) {
			throw new RuntimeException("Exception while closing queue.", e);
		}

		return cmd;
	}

	public void printHelp() {
		super.printHelp(Command.WIPE_QUEUE, "Wipe a queue. The name option is respected.");
	}
}
