package org.icij.extract;

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
public class WipeQueueCli extends CommandCli {

	public WipeQueueCli(Logger logger) {
		super(logger);
	}

	public CommandLine parse(String[] args) throws ParseException, IllegalArgumentException {
		final CommandLine cli = super.parse(args, Command.WIPE_QUEUE);

		return cli;
	}

	public void printHelp() {
		super.printHelp(Command.WIPE_QUEUE, "Wipe a queue. The namespace option is respected.");
	}
}
