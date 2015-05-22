package org.icij.extract;

import java.lang.Runnable;

import java.util.Queue;
import java.util.Arrays;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.IOException;

import java.nio.file.Path;
import java.nio.file.Paths;

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
public class Cli {
	public static final String VERSION = "v1.0.0-beta";
	public static final String DEFAULT_NAMESPACE = "extract";
	public static final String DEFAULT_REDIS_ADDRESS = "127.0.0.1:6379";

	private static final Logger logger = Logger.getLogger(Cli.class.getName());

	private static enum Command {
		QUEUE, WIPE_QUEUE, EXTRACT;

		public String toString() {
			return name().toLowerCase().replace('_', '-');
		}

		public static Command fromString(String command) {
			return Command.valueOf(command.toUpperCase().replace('-', '_'));
		}
	};

	private static enum OutputType {
		FILE, STDOUT, SOLR;

		public String toString() {
			return name().toLowerCase();
		}

		public static OutputType fromString(String outputType) {
			return OutputType.valueOf(outputType.toUpperCase());
		}
	};

	private static enum QueueType {
		MEMORY, REDIS;

		public String toString() {
			return name().toLowerCase();
		}

		public static QueueType fromString(String queueType) {
			return QueueType.valueOf(queueType.toUpperCase());
		}
	}

	private Options getOptionsForCommand(Command command) {
		final Options options = new Options();

		options.addOption(Option.builder("v")
			.desc("Set the log level. Either \"severe\", \"warning\" or \"info\". Defaults to \"warning\".")
			.longOpt("verbosity")
			.hasArg()
			.argName("level")
			.build());

		options.addOption(Option.builder()
			.desc("Set the queue namespace. Defaults to \"" + DEFAULT_NAMESPACE + "\".")
			.longOpt("queue-namespace")
			.hasArg()
			.argName("name")
			.build());

		options.addOption(Option.builder()
			.desc("Set the Redis address. Defaults to " + DEFAULT_REDIS_ADDRESS + ".")
			.longOpt("redis-address")
			.hasArg()
			.argName("address")
			.build());

		switch (command) {
		case QUEUE:
		case WIPE_QUEUE:

			// It doesn't make sense to queue to memory, or wipe a queue from memory.
			// The only valid values here is "redis".
			options.addOption(Option.builder("q")
				.desc("Set the queue backend type. For now, the only valid value is \"redis\". This option is implied for the queue and wipe-queue operations.")
				.longOpt("queue")
				.hasArg()
				.argName("type")
				.build());
			break;
		}

		switch (command) {

		// The extract job can also handle FS scanning for an in-memory queue.
		// It does this automatically when "memory" is specified for the queue type.
		case QUEUE:
		case EXTRACT:
			options.addOption(Option.builder()
				.desc("Time to wait when polling the queue e.g. \"5s\". Defaults to " + Consumer.DEFAULT_TIMEOUT + " " + Consumer.DEFAULT_TIMEOUT_UNIT.name().toLowerCase() + ".")
				.longOpt("queue-timeout")
				.hasArg()
				.argName("timeout")
				.build());

			options.addOption(Option.builder("d")
				.desc("Directory to scan for documents. Defaults to the current directory.")
				.longOpt("directory")
				.hasArg()
				.argName("path")
				.build());

			options.addOption(Option.builder()
				.desc("Glob pattern for matching files e.g. \"*.{tif,pdf}\". Files not matching the pattern will be ignored.")
				.longOpt("include-pattern")
				.hasArg()
				.argName("pattern")
				.build());

			options.addOption(Option.builder()
				.desc("Glob pattern for excluding files and directories. Files and directories matching the pattern will be ignored.")
				.longOpt("exclude-pattern")
				.hasArg()
				.argName("pattern")
				.build());

			options.addOption(Option.builder()
				.desc("Follow symbolic links when scanning for documents. Links are not followed by default.")
				.longOpt("follow-symlinks")
				.build());
			break;
		}

		switch (command) {
		case EXTRACT:
			options.addOption(Option.builder("q")
				.desc("Set the queue backend type. For now, the only valid values are \"redis\" and \"memory\". Defaults to memory.")
				.longOpt("queue")
				.hasArg()
				.argName("type")
				.build());

			options.addOption(Option.builder("t")
				.desc("Number of threads to use while processing. Affects the number of files which are processed concurrently. Defaults to the number of available processors.")
				.longOpt("threads")
				.hasArg()
				.argName("count")
				.type(Number.class)
				.build());

			options.addOption(Option.builder()
				.desc("Set the language used by Tesseract. If none is specified, English is assumed. Multiple languages may be specified, separated by plus characters. Tesseract uses 3-character ISO 639-2 language codes. If set to \"auto\", the language will be detected by running the OCR a second time if a new language is detected using Tika's language detection.")
				.longOpt("ocr-language")
				.hasArg()
				.argName("language")
				.build());

			options.addOption(Option.builder("o")
				.desc("Set the output type. Either \"file\", \"stdout\" or \"solr\".")
				.longOpt("output")
				.hasArg()
				.argName("type")
				.build());

			options.addOption(Option.builder()
				.desc("Set Tika's output encoding. Defaults to UTF-8.")
				.longOpt("output-encoding")
				.hasArg()
				.argName("character set")
				.build());

			options.addOption(Option.builder()
				.desc("Directory to output extracted text.")
				.longOpt("file-directory")
				.hasArg()
				.argName("path")
				.build());
			break;
		}

		return options;
	}

	private Command parseCommand(String command) throws ParseException {
		try {
			return Command.fromString(command);
		} catch (IllegalArgumentException e) {
			logger.log(Level.SEVERE, String.format("\"%s\" is not a valid command.", command), e);
			throw new ParseException("Illegal command.");
		}
	}

	private QueueType parseQueueType(String queueType) throws ParseException {
		try {
			return QueueType.fromString(queueType);
		} catch (IllegalArgumentException e) {
			logger.log(Level.SEVERE, String.format("\"%s\" is not a valid queue type.", queueType), e);
			throw new ParseException("Illegal queue type.");
		}
	}

	public Runnable parse(String[] args) throws ParseException {
		final Options standardOptions = new Options();
		final CommandLineParser parser = new DefaultParser();

		CommandLine cmd = null;
		Runnable job = null;

		standardOptions.addOption(Option.builder("h")
			.longOpt("help")
			.desc("Show help.")
			.hasArg()
			.optionalArg(true)
			.argName("command")
			.build());

		standardOptions.addOption(Option.builder()
			.longOpt("version")
			.build());

		try {
			cmd = parser.parse(standardOptions, args, true);
		} catch (ParseException e) {
			logger.log(Level.SEVERE, "Failed to parse command line arguments.", e);
			throw e;
		}

		// Print the version and exit.
		if (cmd.hasOption("version")) {
			printVersion();
			return job;
		}

		// Print the help for an optional command and exit.
		if (cmd.hasOption('h')) {
			final String help = cmd.getOptionValue('h');

			if (null != help) {
				printHelp(parseCommand(help));
			} else {
				printHelp(standardOptions);
			}

			return job;
		}

		final Command command = parseCommand(args[0]);
		final Options commandOptions = getOptionsForCommand(command);

		args = Arrays.copyOfRange(args, 1, args.length);

		try {
			cmd = parser.parse(commandOptions, args);
		} catch (ParseException e) {
			logger.log(Level.SEVERE, "Failed to parse command line arguments for the " + command + " command.", e);
			throw e;
		}

		if (cmd.hasOption('v')) {
			logger.setLevel(Level.parse(((String) cmd.getOptionValue('v')).toUpperCase()));
		} else {
			logger.setLevel(Level.WARNING);
		}

		switch (command) {
		case QUEUE:
			job = parseQueueOptions(cmd);
			break;
		case WIPE_QUEUE:
			job = parseWipeQueueOptions(cmd);
			break;
		case EXTRACT:
			job = parseExtractOptions(cmd);
			break;
		}

		return job;
	}

	private Runnable parseQueueOptions(CommandLine cmd) {
		Runnable job = null;

		final String directory = (String) cmd.getOptionValue('d', ".");

		final Redisson redisson = createRedisClient(cmd);
		final RQueue<Path> queue = createRedisQueue(cmd, redisson);
		final Scanner scanner = new Scanner(logger, queue);

		setScannerOptions(cmd, scanner);

		job = new Runnable() {

			@Override
			public void run() {
				scanner.scan(Paths.get(directory));
				redisson.shutdown();
			}
		};

		return job;
	}

	private void setScannerOptions(CommandLine cmd, Scanner scanner) {
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

	private Redisson createRedisClient(CommandLine cmd) {
		final Config redissonConfig = new Config();

		redissonConfig
			.useSingleServer()
			.setAddress(cmd.getOptionValue("redis-address", DEFAULT_REDIS_ADDRESS));
		return Redisson.create(redissonConfig);
	}

	private RQueue<Path> createRedisQueue(CommandLine cmd, Redisson redisson) {
		final String queueNamespace = cmd.getOptionValue("queue-namespace", DEFAULT_NAMESPACE);

		return redisson.getQueue(queueNamespace);
	}

	private Runnable parseExtractOptions(CommandLine cmd) throws ParseException {
		Runnable job = null;

		int threads = Consumer.DEFAULT_THREADS;

		if (cmd.hasOption('t')) {
			try {
				threads = ((Number) cmd.getParsedOptionValue("t")).intValue();
			} catch (ParseException e) {
				logger.log(Level.SEVERE, "Invalid value for thread count.", e);
				throw e;
			}
		}

		final OutputType outputType;
		final Spewer spewer;

		try {
			outputType = OutputType.fromString(cmd.getOptionValue('o', "stdout"));
		} catch (IllegalArgumentException e) {
			logger.log(Level.SEVERE, String.format("\"%s\" is not a valid output type.", cmd.getOptionValue('o')), e);
			throw new ParseException(String.format("Illegal output type."));
		}

		if (OutputType.FILE == outputType) {
			spewer = new FileSpewer(logger);

			// TODO: Ensure that the output directory is not the same as the input directory.
			((FileSpewer) spewer).setOutputDirectory(Paths.get((String) cmd.getOptionValue("file-directory", ".")));
		} else {
			spewer = new StdOutSpewer();
		}

		final QueueType queueType = parseQueueType(cmd.getOptionValue('q', "memory"));
		final Consumer consumer;

		if (QueueType.REDIS == queueType) {
			final Redisson redisson = createRedisClient(cmd);
			final RQueue<Path> queue = createRedisQueue(cmd, redisson);

			// With Redis it's simple.
			// Run all the jobs in the queue and exit without waiting for more.
			consumer = new Consumer(logger, queue, spewer, threads);
			consumer.whenDrained(new Runnable() {

				@Override
				public void run() {
					consumer.shutdown();
					redisson.shutdown();
				}
			});

			if (cmd.hasOption("queue-timeout")) {
				consumer.setPollTimeout((String) cmd.getOptionValue("queue-timeout"));
			}

			job = new Runnable() {

				@Override
				public void run() {
					consumer.consume();
				}
			};
		} else {
			final Scanner scanner;
			final String directory;

			// When running in memory mode, don't use a queue.
			// The scanner sends jobs straight to the consumer, the executor of which uses its own internal queue.
			consumer = new Consumer(logger, spewer, threads);
			scanner = new Scanner(logger, consumer);
			directory = (String) cmd.getOptionValue('d', "*");

			setScannerOptions(cmd, scanner);

			job = new Runnable() {

				@Override
				public void run() {
					scanner.scan(Paths.get(directory));
					consumer.shutdown();
				}
			};
		}

		if (cmd.hasOption("output-encoding")) {
			consumer.setOutputEncoding((String) cmd.getOptionValue("output-encoding"));
		}

		if ("auto".equals(((String) cmd.getOptionValue("ocr-language")))) {
			consumer.detectLanguageForOcr();
		} else if (cmd.hasOption("ocr-language")) {
			consumer.setOcrLanguage((String) cmd.getOptionValue("ocr-language"));
		}

		return job;
	}

	private Runnable parseWipeQueueOptions(CommandLine cmd) {
		Runnable job = null;

		return job;
	}

	public void printVersion() {
		System.out.println("Extract " + VERSION);
	}

	private void printHelp(Options options) {
		final HelpFormatter formatter = new HelpFormatter();
		final String header = "\nA cross-platform tool for distributed content-analysis.\n\n";
		final String footer = "\nPlease report issues https://github.com/ICIJ/extract/issues.";

		formatter.printHelp("\033[1mextract\033[0m", header, options, footer, true);
	}

	public void printHelp(Command command) {
		final HelpFormatter formatter = new HelpFormatter();
		final Options options = getOptionsForCommand(command);

		final String footer = "\nExtract is a cross-platform tool for distributed content-analysis.\nPlease report issues https://github.com/ICIJ/extract/issues.";

		String header = null;

		switch (command) {
		case QUEUE:
			header = "\nQueue files for processing later.\n\n";
			break;
		case WIPE_QUEUE:
			header = "\nWipe a queue. The namespace option is respected.\n\n";
			break;
		case EXTRACT:
			header = "\nProcess files.\n\n";
			break;
		}

		formatter.printHelp("\033[1mextract\033[0m " + command, header, options, footer, true);
	}
}
