package org.icij.extract.cli;

import org.icij.extract.core.*;

import java.lang.Runnable;

import java.util.Queue;
import java.util.Arrays;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.IOException;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public class MainCli extends Cli {
	public static final String VERSION = "v1.0.0-beta";

	public MainCli(Logger logger) {
		super(logger, new String[] {
			"h", "version"
		});
	}

	public CommandLine parse(String[] args) throws ParseException {
		CommandLine cmd = null;

		if (0 == args.length) {
			printHelp();
			return cmd;
		}

		cmd = DEFAULT_PARSER.parse(options, args, true);

		// Print the version string.
		if (cmd.hasOption("version")) {
			printVersion();
			return cmd;
		}

		// Print general help or for an optional command.
		if (cmd.hasOption('h') && null == cmd.getOptionValue('h')) {
			printHelp();
		} else if (cmd.hasOption('h')) {
			Command.parse(cmd.getOptionValue('h')).createCli(logger).printHelp();

		// Run the given command.
		// Remove the command from the beginning of the arguments array.
		} else {
			Command.parse(args[0]).createCli(logger).parse(Arrays.copyOfRange(args, 1, args.length));
		}

		return cmd;
	}

	public void printVersion() {
		System.out.println("Extract " + VERSION);
	}

	public void printHelp() {
		final HelpFormatter formatter = new HelpFormatter();
		final String header = "\nA cross-platform tool for distributed content-analysis.\n\n";
		final String footer = "\nPlease report issues https://github.com/ICIJ/extract/issues.";

		formatter.printHelp("\033[1mextract\033[0m", header, options, footer, true);
	}
}
