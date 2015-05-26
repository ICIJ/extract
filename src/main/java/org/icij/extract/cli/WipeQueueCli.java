package org.icij.extract.cli;

import org.icij.extract.core.*;

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
		super(logger, new String[] {
			"v", "q", "redis-namespace", "redis-address"
		});
	}

	public CommandLine parse(String[] args) throws ParseException, IllegalArgumentException {
		final CommandLine cmd = super.parse(args);

		return cmd;
	}

	public void printHelp() {
		super.printHelp(Command.WIPE_QUEUE, "Wipe a queue. The namespace option is respected.");
	}
}
