package org.icij.extract.cli;

import org.icij.extract.core.*;

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

	public Cli(Logger logger, String[] options) {
		this.logger = logger;

		for (String name: options) {
			this.options.addOption(createOption(name));
		}
	}

	protected Option createOption(String name) {
		switch (name) {

		case "v": return Option.builder("v")
			.desc("Set the log level. Either \"severe\", \"warning\" or \"info\". Defaults to \"warning\".")
			.longOpt("verbosity")
			.hasArg()
			.argName("level")
			.build();

		default:
			throw new IllegalArgumentException("Unknown option: " + name + ".");
		}
	}

	protected CommandLine parse(String[] args) throws ParseException, IllegalArgumentException, RuntimeException {
		final CommandLine cli = DEFAULT_PARSER.parse(options, args);

		if (cli.hasOption('v')) {
			logger.setLevel(Level.parse(((String) cli.getOptionValue('v')).toUpperCase(Locale.ROOT)));
		} else {
			logger.setLevel(Level.WARNING);
		}

		return cli;
	}

	protected abstract void printHelp();

	protected void printHelp(Command command, String description) {
		final HelpFormatter formatter = new HelpFormatter();

		final String footer = "\nExtract is a cross-platform tool for distributed content-analysis.\nPlease report issues at: https://github.com/ICIJ/extract/issues.";
		final String header = "\n" + description + "\n\n";

		formatter.printHelp("\033[1mextract\033[0m " + command, header, options, footer, true);
	}
}
