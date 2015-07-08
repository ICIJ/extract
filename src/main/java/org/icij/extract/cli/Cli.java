package org.icij.extract.cli;

import org.icij.extract.core.*;
import org.icij.extract.cli.options.*;

import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;

import java.nio.file.Path;

import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import org.redisson.Config;
import org.redisson.Redisson;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public abstract class Cli {

	public static final CommandLineParser DEFAULT_PARSER = new DefaultParser();

	private static Redisson redisson = null;

	public static Redisson getRedisson(CommandLine cli) {
		final Config config;

		if (null != redisson) {
			return redisson;
		}

		if (!cli.hasOption("redis-address")) {
			return Redisson.create();
		}

		config = new Config();

		// TODO: Create a cluster if more than one address is given.
		config.useSingleServer().setAddress(cli.getOptionValue("redis-address"));

		redisson = Redisson.create(config);
		return redisson;
	}

	protected final Logger logger;
	protected final Options options = new Options();

	public Cli(Logger logger, OptionSet... optionSets) {
		this.logger = logger;

		new LoggerOptionSet().addToOptions(options);

		for (OptionSet optionSet: optionSets) {
			optionSet.addToOptions(options);
		}
	}

	protected CommandLine parse(String[] args) throws ParseException, IllegalArgumentException, RuntimeException {
		final CommandLine cmd = DEFAULT_PARSER.parse(options, args);

		LoggerOptionSet.configureLogger(cmd, logger);

		return cmd;
	}

	protected abstract void printHelp();

	protected void printHelp(Command command, String description) {
		final HelpFormatter formatter = new HelpFormatter();

		final String footer = "\nExtract is a cross-platform tool for distributed content-analysis.\nPlease report issues at: https://github.com/ICIJ/extract/issues.";
		final String header = "\n" + description + "\n\n";

		formatter.printHelp("\033[1mextract\033[0m " + command, header, options, footer, true);
	}
}
