package org.icij.extract;

import java.util.logging.Level;
import java.util.logging.Logger;

import java.nio.file.Path;

import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import org.redisson.Config;
import org.redisson.Redisson;
import org.redisson.core.RQueue;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public abstract class Cli {

	public static final CommandLineParser DEFAULT_PARSER = new DefaultParser();
	public static final String DEFAULT_NAMESPACE = "extract";

	protected final Logger logger;

	public static Redisson createRedisClient(CommandLine cli) {
		if (!cli.hasOption("redis-address")) {
			return Redisson.create();
		}

		final Config config = new Config();

		// TODO: Create a cluster if more than one address is given.
		config.useSingleServer().setAddress(cli.getOptionValue("redis-address"));

		return Redisson.create(config);
	}

	public static RQueue<Path> createRedisQueue(CommandLine cli, Redisson redisson) {
		return redisson.getQueue(cli.getOptionValue("queue-namespace", DEFAULT_NAMESPACE) + ":queue");
	}

	public Cli(Logger logger) {
		this.logger = logger;
	}

	protected abstract CommandLine parse(String[] args) throws ParseException, IllegalArgumentException;

	protected CommandLine parse(String[] args, Command command) throws ParseException {
		final Options options = MainCli.createOptionsForCommand(command);
		final CommandLine cli = DEFAULT_PARSER.parse(options, args);

		if (cli.hasOption('v')) {
			logger.setLevel(Level.parse(((String) cli.getOptionValue('v')).toUpperCase()));
		} else {
			logger.setLevel(Level.WARNING);
		}

		return cli;
	}

	protected abstract void printHelp();

	protected void printHelp(Command command, String description) {
		final HelpFormatter formatter = new HelpFormatter();
		final Options options = MainCli.createOptionsForCommand(command);

		final String footer = "\nExtract is a cross-platform tool for distributed content-analysis.\nPlease report issues https://github.com/ICIJ/extract/issues.";
		final String header = "\n" + description + ".\n\n";

		formatter.printHelp("\033[1mextract\033[0m " + command, header, options, footer, true);
	}
}
