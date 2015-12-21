package org.icij.extract.cli;

import org.icij.extract.cli.options.*;

import java.util.logging.Logger;

import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public abstract class Cli {

	public static final CommandLineParser DEFAULT_PARSER = new DefaultParser();

	protected final Logger logger;
	protected final Options options = new Options();

	public Cli(final Logger logger, OptionSet... optionSets) {
		this.logger = logger;

		new LoggerOptionSet().addToOptions(options);

		for (OptionSet optionSet: optionSets) {
			optionSet.addToOptions(options);
		}
	}

	protected CommandLine parse(final String[] args) throws ParseException, IllegalArgumentException {
		final CommandLine cmd = DEFAULT_PARSER.parse(options, args);

		LoggerOptionSet.configureLogger(cmd, logger);

		return cmd;
	}

	protected abstract void printHelp();

	protected void printHelp(final Command command, final String description) {
		printHelp(command, description, null);
	}

	protected void printHelp(final Command command, final String description, final String arguments) {
		final HelpFormatter formatter = new HelpFormatter();

		final String footer = "\nPlease report issues at: https://github.com/ICIJ/extract/issues.";
		final String header = "\n" + description + "\n\n" +
			"Part of Extract, a cross-platform tool for distributed content-analysis.\n\n" +
			"\033[1mOptions\033[0m\n\n";

		String syntax = "\033[1mextract\033[0m " + command + " [options]";
		if (null != arguments) {
			syntax += " " + arguments;
		}

		formatter.printHelp(syntax, header, options, footer, false);
	}
}
