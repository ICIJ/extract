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
		printHelp(command, description, null);
	}

	protected void printHelp(Command command, String description, String arguments) {
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
