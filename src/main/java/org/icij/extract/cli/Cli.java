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

		case "n": return Option.builder()
			.desc("Set the name for the job. This affects the names of queues and reports in their respective backends, avoiding conflicts. Defaults to \"extract\".")
			.longOpt("name")
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
			.desc("Time to wait when polling the queue e.g. \"5s\". Defaults to " + PollingConsumer.DEFAULT_TIMEOUT + " " + PollingConsumer.DEFAULT_TIMEOUT_UNIT.name().toLowerCase(Locale.ROOT) + ".")
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

		case "ocr-disabled": return Option.builder()
			.desc("Disable automatic OCR. On by default.")
			.longOpt(name)
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
			.desc("Directory to output extracted text. Defaults to the current directory.")
			.longOpt(name)
			.hasArg()
			.argName("path")
			.build();

		case "t": return Option.builder("t")
			.desc("Solr field for extracted text. Defaults to \"" + SolrSpewer.DEFAULT_TEXT_FIELD + "\".")
			.longOpt("solr-text-field")
			.hasArg()
			.argName("name")
			.build();

		case "f": return Option.builder("f")
			.desc("Solr field for the file path. Defaults to \"" + SolrSpewer.DEFAULT_PATH_FIELD + "\".")
			.longOpt("solr-path-field")
			.hasArg()
			.argName("name")
			.build();

		case "i": return Option.builder("i")
			.desc("Solr field for an automatically generated identifier. The ID for the same file is guaranteed not to change if the path doesn't change.")
			.longOpt("solr-id-field")
			.hasArg()
			.argName("name")
			.build();

		case "solr-id-algorithm": return Option.builder()

			// The standard names are defined in the Oracle Standard Algorithm Name Documentation:
			// http://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html#MessageDigest
			.desc("The hashing algorithm used for generating Solr document identifiers e.g. \"MD5\" or \"SHA-1\". Defaults to SHA-256.")
			.longOpt(name)
			.hasArg()
			.argName("name")
			.build();

		case "solr-commit-interval": return Option.builder()
			.desc("Commit to Solr every time the specified number of documents is added. Disabled by default. Consider using the \"autoCommit\" \"maxDocs\" directive in your Solr update handler configuration instead.")
			.longOpt(name)
			.hasArg()
			.argName("interval")
			.type(Number.class)
			.build();

		case "solr-commit-within": return Option.builder()
			.desc("Instruct Solr to automatically commit a document after the specified milliseconds have elapsed since it was added. Disabled by default. Consider using the \"autoCommit\" \"maxTime\" directive in your Solr update handler configuration instead.")
			.longOpt(name)
			.hasArg()
			.argName("interval")
			.type(Number.class)
			.build();

		case "solr-pin-certificate": return Option.builder()
			.desc("The Solr server's public certificate, used for certificate pinning. Supported formats are PEM, DER, PKCS #12 and JKS.")
			.longOpt(name)
			.hasArg()
			.argName("path")
			.build();

		case "solr-verify-host": return Option.builder()
			.desc("Verify the server's public certificate against the specified host. Use the wildcard \"*\" to disable verification.")
			.longOpt(name)
			.hasArg()
			.argName("hostname")
			.build();

		case "r": return Option.builder("r")
			.desc("Set the reporter backend type. This is used to skip files that have already been extracted and outputted successfully. For now, the only valid values are \"redis\" and \"none\". Defaults to none.")
			.longOpt("reporter")
			.hasArg()
			.argName("type")
			.build();

		case "reporter-status": return Option.builder()
			.desc("Only dump reports matching the given status.")
			.longOpt(name)
			.hasArg()
			.argName("status")
			.type(Number.class)
			.required(true)
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
