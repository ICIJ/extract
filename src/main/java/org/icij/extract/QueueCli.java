package org.icij.extract;

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
public class QueueCli extends CommandCli {

	public static void setScannerOptions(CommandLine cli, Scanner scanner) {
		if (cli.hasOption("include-pattern")) {
			scanner.setIncludeGlob((String) cli.getOptionValue("include-pattern"));
		}

		if (cli.hasOption("exclude-pattern")) {
			scanner.setExcludeGlob((String) cli.getOptionValue("exclude-pattern"));
		}

		if (cli.hasOption("follow-symlinks")) {
			scanner.followSymLinks();
		}
	}

	public QueueCli(Logger logger) {
		super(logger);
	}

	public CommandLine parse(String[] args) throws ParseException, IllegalArgumentException {
		final CommandLine cli = super.parse(args, Command.QUEUE);

		final Redisson redisson = getRedisson(cli);
		final RQueue<Path> queue = redisson.getQueue(cli.getOptionValue("redis-namespace", "extract") + ":queue");
		final Scanner scanner = new QueueingScanner(logger, queue);

		setScannerOptions(cli, scanner);

		final String directory = (String) cli.getOptionValue('d', ".");

		scanner.scan(Paths.get(directory));
		redisson.shutdown();

		return cli;
	}

	public void printHelp() {
		super.printHelp(Command.QUEUE, "Queue files for processing later.");
	}
}
