package org.icij.extract;

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
public class MainCli {
	public static final String VERSION = "v1.0.0-beta";

	public static Options createOptionsForCommand(Command command) {
		final Options options = new Options();

		options.addOption(Option.builder("v")
			.desc("Set the log level. Either \"severe\", \"warning\" or \"info\". Defaults to \"warning\".")
			.longOpt("verbosity")
			.hasArg()
			.argName("level")
			.build()).addOption(Option.builder()
			.desc("Set the queue namespace. Defaults to \"" + Cli.DEFAULT_NAMESPACE + "\".")
			.longOpt("queue-namespace")
			.hasArg()
			.argName("name")
			.build()).addOption(Option.builder()
			.desc("Set the Redis address. Defaults to 127.0.0.1:6379.")
			.longOpt("redis-address")
			.hasArg()
			.argName("address")
			.build());

		if (Command.QUEUE == command || Command.WIPE_QUEUE == command) {

			// It doesn't make sense to queue to memory, or wipe a queue from memory.
			// The only valid values here is "redis".
			options.addOption(Option.builder("q")
				.desc("Set the queue backend type. For now, the only valid value is \"redis\". This option is implied for the queue and wipe-queue commands.")
				.longOpt("queue")
				.hasArg()
				.argName("type")
				.build());
		}

		// The extract job can also handle FS scanning for an in-memory queue.
		// It does this automatically when "memory" is specified for the queue type.
		if (Command.QUEUE == command || Command.EXTRACT == command) {
			options.addOption(Option.builder("d")
				.desc("Directory to scan for documents. Defaults to the current directory.")
				.longOpt("directory")
				.hasArg()
				.argName("path")
				.build()).addOption(Option.builder()
				.desc("Glob pattern for matching files e.g. \"*.{tif,pdf}\". Files not matching the pattern will be ignored.")
				.longOpt("include-pattern")
				.hasArg()
				.argName("pattern")
				.build()).addOption(Option.builder()
				.desc("Glob pattern for excluding files and directories. Files and directories matching the pattern will be ignored.")
				.longOpt("exclude-pattern")
				.hasArg()
				.argName("pattern")
				.build()).addOption(Option.builder()
				.desc("Follow symbolic links when scanning for documents. Links are not followed by default.")
				.longOpt("follow-symlinks")
				.build());
		}

		if (Command.EXTRACT == command) {
			options.addOption(Option.builder("q")
				.desc("Set the queue backend type. For now, the only valid values are \"redis\" and \"memory\". Defaults to memory.")
				.longOpt("queue")
				.hasArg()
				.argName("type")
				.build()).addOption(Option.builder()
				.desc("Time to wait when polling the queue e.g. \"5s\". Defaults to " + PollingConsumer.DEFAULT_TIMEOUT + " " + PollingConsumer.DEFAULT_TIMEOUT_UNIT.name().toLowerCase() + ".")
				.longOpt("queue-poll")
				.hasArg()
				.argName("duration")
				.build()).addOption(Option.builder("p")
				.desc("The number of files which are processed concurrently. Defaults to the number of available processors.")
				.longOpt("parallel")
				.hasArg()
				.argName("count")
				.type(Number.class)
				.build()).addOption(Option.builder()
				.desc("Set the language used by Tesseract. If none is specified, English is assumed. Multiple languages may be specified, separated by plus characters. Tesseract uses 3-character ISO 639-2 language codes.")
				.longOpt("ocr-language")
				.hasArg()
				.argName("language")
				.build()).addOption(Option.builder("o")
				.desc("Set the output type. Either \"file\", \"stdout\" or \"solr\".")
				.longOpt("output")
				.hasArg()
				.argName("type")
				.build()).addOption(Option.builder()
				.desc("Set Tika's output encoding. Defaults to UTF-8.")
				.longOpt("output-encoding")
				.hasArg()
				.argName("character set")
				.build()).addOption(Option.builder()
				.desc("Directory to output extracted text.")
				.longOpt("file-output-directory")
				.hasArg()
				.argName("path")
				.build()).addOption(Option.builder("s")
				.desc("Solr server address. Required if outputting to Solr.")
				.longOpt("solr")
				.hasArg()
				.argName("address")
				.build()).addOption(Option.builder("t")
				.desc("Solr field for extracted text. Defaults to \"" + SolrSpewer.DEFAULT_FIELD + "\".")
				.longOpt("solr-text-field")
				.hasArg()
				.argName("address")
				.build()).addOption(Option.builder()
				.desc("Commit to Solr after every specified number of files are added. Defaults to \"" + SolrSpewer.DEFAULT_INTERVAL + "\".")
				.longOpt("solr-commit-interval")
				.hasArg()
				.argName("interval")
				.type(Number.class)
				.build());
		}

		return options;
	}

	private final Logger logger;
	private final Options options = new Options();

	public MainCli(Logger logger) {
		this.logger = logger;

		options.addOption(Option.builder("h")
			.longOpt("help")
			.desc("Show help.")
			.hasArg()
			.optionalArg(true)
			.argName("command")
			.build()).addOption(Option.builder()
			.longOpt("version")
			.build());
	}

	public void parse(String[] args) throws ParseException {
		final CommandLine cli;

		cli = Cli.DEFAULT_PARSER.parse(options, args, true);

		// Print the version string.
		if (cli.hasOption("version")) {
			printVersion();

		// Print general help or for an optional command.
		} else if (cli.hasOption('h')) {
			printHelp(cli.getOptionValue('h'));

		// Run the given command.
		// Remove the command from the beginning of the arguments array.
		} else {
			Command.parse(args[0]).createCli(logger).parse(Arrays.copyOfRange(args, 1, args.length));
		}
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

	private void printHelp(String command) {
		if (null != command) {
			Command.parse(command).createCli(logger).printHelp();
		} else {
			printHelp();
		}
	}
}
