package org.icij.extract.cli;

import org.icij.extract.core.*;
import org.icij.extract.cli.options.*;

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
		super(logger);

		options.addOption(Option.builder("h")
				.desc("Display help for a command.")
				.longOpt("help")
				.hasArg()
				.optionalArg(true)
				.argName("command")
				.build())

			.addOption(Option.builder()
				.desc("Display the version number.")
				.longOpt("version")
				.build());
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

		final String[] commands = new String[Command.values().length];
		int i = 0;

		for (Command command : Command.values()) {
			commands[i++] = command.toString();
		}

		final String header = "\nA cross-platform tool for distributed content-analysis " +
			"by the data team at the International Consortium of Investigative Journalists.\n\n" +
			"\033[1mCommands\033[0m\n\n " + String.join("\n ", commands) + "\n\n" +
			"\033[1mOptions\033[0m\n\n";
		final String footer = "\nPlease report issues at: https://github.com/ICIJ/extract/issues.";

		formatter.printHelp("\033[1mextract\033[0m [command] [options]\n" +
			formatter.getSyntaxPrefix() + "\033[1mextract\033[0m -h [command]\n" +
			formatter.getSyntaxPrefix() + "\033[1mextract\033[0m --version",
			header, options, footer, false);
	}
}
