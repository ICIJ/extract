package org.icij.extract.cli;

import org.icij.extract.core.*;

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

	private Option createOption(String name) {
		switch (name) {

		case "h": return Option.builder("h")
			.desc("Display this message.")
			.longOpt("help")
			.hasArg()
			.optionalArg(true)
			.argName("command")
			.build();

		case "version": return Option.builder()
			.desc("Display the version number.")
			.longOpt("version")
			.build();

		case "v": return Option.builder("v")
			.desc("Set the log level. Either \"severe\", \"warning\" or \"info\". Defaults to \"warning\".")
			.longOpt("verbosity")
			.hasArg()
			.argName("level")
			.build();

		case "q": return Option.builder("q")
			.desc("Set the queue backend type. For now, the only valid values are \"redis\" and \"none\". Defaults to none when extracting and Redis for the queue command.")
			.longOpt("queue")
			.hasArg()
			.argName("type")
			.build();

		case "d": return Option.builder("d")
			.desc("Directory to scan for documents. Defaults to the current directory.")
			.longOpt("directory")
			.hasArg()
			.argName("path")
			.build();

		case "redis-namespace": return Option.builder()
			.desc("Set the Redis namespace. Defaults to \"extract\".")
			.longOpt(name)
			.hasArg()
			.argName("name")
			.build();

		case "redis-address": return Option.builder()
			.desc("Set the Redis backend address. Defaults to 127.0.0.1:6379.")
			.longOpt(name)
			.hasArg()
			.argName("address")
			.build();

		case "include-pattern": return Option.builder()
			.desc("Glob pattern for matching files e.g. \"*.{tif,pdf}\". Files not matching the pattern will be ignored.")
			.longOpt(name)
			.hasArg()
			.argName("pattern")
			.build();

		case "exclude-pattern": return Option.builder()
			.desc("Glob pattern for excluding files and directories. Files and directories matching the pattern will be ignored.")
			.longOpt(name)
			.hasArg()
			.argName("pattern")
			.build();

		case "follow-symlinks": return Option.builder()
			.desc("Follow symbolic links when scanning for documents. Links are not followed by default.")
			.longOpt(name)
			.build();

		case "queue-poll": return Option.builder()
			.desc("Time to wait when polling the queue e.g. \"5s\". Defaults to " + PollingConsumer.DEFAULT_TIMEOUT + " " + PollingConsumer.DEFAULT_TIMEOUT_UNIT.name().toLowerCase() + ".")
			.longOpt(name)
			.hasArg()
			.argName("duration")
			.build();

		case "p": return Option.builder("p")
			.desc("The number of files which are processed concurrently. Defaults to the number of available processors.")
			.longOpt("parallel")
			.hasArg()
			.argName("count")
			.type(Number.class)
			.build();

		case "ocr-language": return Option.builder()
			.desc("Set the language used by Tesseract. If none is specified, English is assumed. Multiple languages may be specified, separated by plus characters. Tesseract uses 3-character ISO 639-2 language codes.")
			.longOpt(name)
			.hasArg()
			.argName("language")
			.build();

		case "o": return Option.builder("o")
			.desc("Set the output type. Either \"file\", \"stdout\" or \"solr\".")
			.longOpt("output")
			.hasArg()
			.argName("type")
			.build();

		case "output-encoding": return Option.builder()
			.desc("Set Tika's output encoding. Defaults to UTF-8.")
			.longOpt(name)
			.hasArg()
			.argName("character set")
			.build();

		case "file-output-directory": return Option.builder()
			.desc("Directory to output extracted text.")
			.longOpt(name)
			.hasArg()
			.argName("path")
			.build();

		case "s": return Option.builder("s")
			.desc("Solr server address. Required if outputting to Solr.")
			.longOpt("solr-address")
			.hasArg()
			.argName("address")
			.build();

		case "t": return Option.builder("t")
			.desc("Solr field for extracted text. Defaults to \"" + SolrSpewer.DEFAULT_FIELD + "\".")
			.longOpt("solr-text-field")
			.hasArg()
			.argName("address")
			.build();

		case "solr-commit-interval": return Option.builder()
			.desc("Commit to Solr after every specified number of files are added. Defaults to \"" + SolrSpewer.DEFAULT_INTERVAL + "\".")
			.longOpt(name)
			.hasArg()
			.argName("interval")
			.type(Number.class)
			.build();

		case "r": return Option.builder("r")
			.desc("Set the reporter backend type. This is used to skip files that have already been extracted and outputted successfully. For now, the only valid values are \"redis\" and \"none\". Defaults to none.")
			.longOpt("reporter")
			.hasArg()
			.argName("type")
			.build();

		default:
			throw new IllegalArgumentException("Unknown option: " + name + ".");
		}
	}

	protected CommandLine parse(String[] args) throws ParseException, IllegalArgumentException {
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

		final String footer = "\nExtract is a cross-platform tool for distributed content-analysis.\nPlease report issues https://github.com/ICIJ/extract/issues.";
		final String header = "\n" + description + "\n\n";

		formatter.printHelp("\033[1mextract\033[0m " + command, header, options, footer, true);
	}
}
