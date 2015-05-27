package org.icij.extract.cli;

import org.icij.extract.core.*;

import java.util.logging.Logger;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.redisson.Redisson;
import org.redisson.core.RQueue;

import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.CommandLine;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public class QueueCli extends Cli {

	public static void setScannerOptions(CommandLine cmd, Scanner scanner) {
		if (cmd.hasOption("include-pattern")) {
			scanner.setIncludeGlob((String) cmd.getOptionValue("include-pattern"));
		}

		if (cmd.hasOption("exclude-pattern")) {
			scanner.setExcludeGlob((String) cmd.getOptionValue("exclude-pattern"));
		}

		if (cmd.hasOption("follow-symlinks")) {
			scanner.followSymLinks();
		}
	}

	public QueueCli(Logger logger) {
		super(logger, new String[] {
			"v", "q", "d", "redis-namespace", "redis-address", "include-pattern", "exclude-pattern", "follow-symlinks", "queue-poll"
		});
	}

	public CommandLine parse(String[] args) throws ParseException, IllegalArgumentException {
		final CommandLine cmd = super.parse(args);

		final Redisson redisson = getRedisson(cmd);
		final RQueue<String> queue = redisson.getQueue(cmd.getOptionValue("redis-namespace", "extract") + ":queue");
		final Scanner scanner = new QueueingScanner(logger, queue);

		setScannerOptions(cmd, scanner);

		final String directory = (String) cmd.getOptionValue('d', ".");

		scanner.scan(Paths.get(directory));
		redisson.shutdown();

		return cmd;
	}

	public void printHelp() {
		super.printHelp(Command.QUEUE, "Queue files for processing later.");
	}
}
