package org.icij.extract.cli;

import org.icij.extract.core.*;

import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import java.io.IOException;

import java.nio.file.Path;
import java.nio.file.Paths;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;

import java.security.NoSuchAlgorithmException;

import org.redisson.Redisson;
import org.redisson.core.RMap;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.CommandLine;

import org.apache.http.impl.client.CloseableHttpClient;

import org.apache.solr.client.solrj.impl.HttpSolrClient;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public class SpewCli extends Cli {

	public SpewCli(Logger logger) {
		super(logger, new String[] {
			"v", "q", "n", "redis-address", "include-pattern", "exclude-pattern", "follow-symlinks", "queue-poll", "p", "ocr-language", "ocr-disabled", "ocr-timeout", "o", "output-encoding", "output-base", "output-metadata", "file-output-directory", "s", "t", "f", "i", "solr-id-algorithm", "solr-metadata-prefix", "solr-commit-interval", "solr-commit-within", "solr-pin-certificate", "solr-verify-host", "r", "e", "output-format"
		});
	}

	protected Option createOption(String name) {
		switch (name) {

		case "p": return Option.builder("p")
			.desc("The number of files which are processed concurrently. Defaults to the number of available processors.")
			.longOpt("parallel")
			.hasArg()
			.argName("count")
			.type(Number.class)
			.build();

		case "q": return Option.builder("q")
			.desc("Set the queue backend type. For now, the only valid values are \"redis\" and \"none\". Defaults to none when extracting.")
			.longOpt("queue")
			.hasArg()
			.argName("type")
			.build();

		case "queue-poll": return Option.builder()
			.desc(String.format("Time to wait when polling the queue e.g. \"5s\" or \"1m\". Defaults to %s.",
				PollingConsumer.DEFAULT_TIMEOUT))
			.longOpt(name)
			.hasArg()
			.argName("duration")
			.build();

		case "n": return Option.builder("n")
			.desc("If using a queue backend, set the name for the queue. Defaults to \"extract\".")
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

		case "s": return Option.builder("s")
			.desc("Solr server address. Required if outputting to Solr.")
			.longOpt("solr-address")
			.hasArg()
			.argName("address")
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

		case "ocr-timeout": return Option.builder()
			.desc("Set the timeout for the Tesseract process to finish e.g. \"5s\" or \"1m\". Default is 120s.")
			.longOpt(name)
			.hasArg()
			.argName("duration")
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

		case "output-base": return Option.builder()
			.desc("This is useful if your local path contains tokens that you want to strip from the path included in the output. For example, if you're working with a path that looks like \"/home/user/data\", specify \"/home/user/\" as the value for this option so that all outputted paths start with \"data/\".")
			.longOpt(name)
			.hasArg()
			.argName("path")
			.build();

		case "output-metadata": return Option.builder()
			.desc("Output metadata along with extracted text. For the \"file\" output type, a corresponding JSON file is created for every input file. With Solr, metadata fields are set using an optional prefix.")
			.longOpt(name)
			.build();

		case "output-format": return Option.builder()
			.desc("Set the output format. Either \"text\" or \"HTML\". Defaults to text output.")
			.longOpt(name)
			.hasArg()
			.argName("format")
			.build();

		case "e": return Option.builder("e")
			.desc("Set the embed handling mode. Either \"ignore\", \"extract\" or \"embed\". When set to extract, embeds are parsed and the output is inlined into the main output. In embed mode, embeds are not parsed but are inlined as a data URI representation of the raw embed data. The latter mode only applies when the output format is set to HTML. Defaults to extracting.")
			.longOpt("embed-handling")
			.hasArg()
			.argName("mode")
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
			.desc("Solr field for an automatically generated identifier. The ID for the same file is guaranteed not to change if the path doesn't change. Defaults to \"" + SolrSpewer.DEFAULT_ID_FIELD + "\".")
			.longOpt("solr-id-field")
			.hasArg()
			.argName("name")
			.build();

		case "solr-id-algorithm": return Option.builder()

			// The standard names are defined in the Oracle Standard Algorithm Name Documentation:
			// http://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html#MessageDigest
			.desc("The hashing algorithm used for generating Solr document identifiers e.g. \"MD5\" or \"SHA-256\". Turned off by default, so no ID is added.")
			.longOpt(name)
			.hasArg()
			.argName("name")
			.build();

		case "solr-metadata-prefix": return Option.builder()
			.desc("Prefix for metadata fields added to Solr. Defaults to \"" + SolrSpewer.DEFAULT_METADATA_FIELD_PREFIX + "\".")
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
			.desc("Instruct Solr to automatically commit a document after the specified amount of time has elapsed since it was added. Disabled by default. Consider using the \"autoCommit\" \"maxTime\" directive in your Solr update handler configuration instead.")
			.longOpt(name)
			.hasArg()
			.argName("duration")
			.build();

		case "r": return Option.builder("r")
			.desc("Set the reporter backend type. This is used to skip files that have already been extracted and outputted successfully. For now, the only valid values are \"redis\" and \"none\". Defaults to none.")
			.longOpt("reporter")
			.hasArg()
			.argName("type")
			.build();

		default:
			return super.createOption(name);
		}
	}

	public CommandLine parse(String[] args) throws ParseException, IllegalArgumentException {
		final CommandLine cmd = super.parse(args);

		int threads = Consumer.DEFAULT_THREADS;

		if (cmd.hasOption('p')) {
			try {
				threads = ((Number) cmd.getParsedOptionValue("p")).intValue();
			} catch (ParseException e) {
				throw new IllegalArgumentException("Invalid value for thread count.");
			}
		}

		logger.info("Processing up to " + threads + " file(s) in parallel.");

		final OutputType outputType;
		final Spewer spewer;

		try {
			outputType = OutputType.fromString(cmd.getOptionValue('o', "stdout"));
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException(String.format("\"%s\" is not a valid output type.", cmd.getOptionValue('o')));
		}

		if (OutputType.SOLR == outputType) {
			if (!cmd.hasOption('s')) {
				throw new IllegalArgumentException("The -s option is required when outputting to Solr.");
			}

			final CloseableHttpClient httpClient = ClientUtils.createHttpClient(cmd.getOptionValue("solr-pin-certificate"), cmd.getOptionValue("solr-verify-host"));

			spewer = new SolrSpewer(logger, new HttpSolrClient(cmd.getOptionValue('s'), httpClient));
			if (cmd.hasOption('t')) {
				((SolrSpewer) spewer).setTextField(cmd.getOptionValue('t'));
			}

			if (cmd.hasOption('f')) {
				((SolrSpewer) spewer).setPathField(cmd.getOptionValue('f'));
			}

			if (cmd.hasOption('i')) {
				((SolrSpewer) spewer).setIdField(cmd.getOptionValue('i'));
			}

			if (cmd.hasOption("solr-id-algorithm")) {
				try {
					((SolrSpewer) spewer).setIdAlgorithm(cmd.getOptionValue("solr-id-algorithm"));
				} catch (NoSuchAlgorithmException e) {
					throw new IllegalArgumentException(String.format("Hashing algorithm \"%s\" not available on this platform.",
						cmd.getOptionValue("solr-id-algorithm")));
				}
			}

			if (cmd.hasOption("solr-metadata-prefix")) {
				((SolrSpewer) spewer).setMetadataFieldPrefix(cmd.getOptionValue("solr-metadata-prefix"));
			}

			if (cmd.hasOption("solr-commit-interval")) {
				((SolrSpewer) spewer).setCommitInterval(((Number) cmd.getParsedOptionValue("solr-commit-interval")).intValue());
			}

			if (cmd.hasOption("solr-commit-within")) {
				((SolrSpewer) spewer).setCommitWithin(cmd.getOptionValue("solr-commit-within"));
			}
		} else if (OutputType.FILE == outputType) {
			spewer = new FileSpewer(logger, Paths.get((String) cmd.getOptionValue("file-output-directory", ".")));
		} else {
			spewer = new PrintStreamSpewer(logger, System.out);
		}

		if (cmd.hasOption("output-base")) {
			spewer.setOutputBase(cmd.getOptionValue("output-base"));
		}

		if (cmd.hasOption("output-metadata")) {
			spewer.outputMetadata(true);
		}

		final QueueType queueType = QueueType.parse(cmd.getOptionValue('q'));
		final ReporterType reporterType = ReporterType.parse(cmd.getOptionValue('r'));
		final Extractor extractor = new Extractor(logger);

		final BlockingQueue<String> queue;

		if (QueueType.REDIS == queueType) {
			queue = getRedisson(cmd).getBlockingQueue(cmd.getOptionValue('n', "extract") + ":queue");
		} else {

			// Create a classic "bounded buffer", in which a fixed-sized array holds elements inserted by producers and extracted by consumers.
			queue = new ArrayBlockingQueue<String>(threads * 2);
		}

		final PollingConsumer consumer = new PollingConsumer(logger, queue, spewer, extractor, threads);

		if (cmd.hasOption("queue-poll")) {
			consumer.setPollTimeout((String) cmd.getOptionValue("queue-poll"));
		}

		if (cmd.hasOption("output-encoding")) {
			consumer.setOutputEncoding(cmd.getOptionValue("output-encoding"));
		}

		if (cmd.hasOption("output-format")) {
			extractor.setOutputFormat(Extractor
				.OutputFormat.parse(cmd.getOptionValue("output-format")));
		}

		if (cmd.hasOption('e')) {
			extractor.setEmbedHandling(Extractor
				.EmbedHandling.parse(cmd.getOptionValue('e')));
		}

		if (cmd.hasOption("ocr-language")) {
			extractor.setOcrLanguage(cmd.getOptionValue("ocr-language"));
		}

		if (cmd.hasOption("ocr-timeout")) {
			extractor.setOcrTimeout(cmd.getOptionValue("ocr-timeout"));
		}

		if (cmd.hasOption("ocr-disabled")) {
			extractor.disableOcr();
		}

		if (OutputType.FILE == outputType &&
			extractor.getOutputFormat() == Extractor.OutputFormat.HTML) {

			((FileSpewer) spewer).setOutputExtension("html");
		}

		if (ReporterType.REDIS == reporterType) {
			final RMap<String, Integer> report = getRedisson(cmd).getMap(cmd.getOptionValue('n', "extract") + ":report");
			final Reporter reporter = new Reporter(logger, report);

			logger.info("Using Redis reporter.");
			consumer.setReporter(reporter);
		}

		final Thread shutdownHook = new Thread() {
			public void run() {
				logger.warning("Shutdown hook triggered. Please wait for the process to finish cleanly.");

				try {
					consumer.stop();
					consumer.awaitTermination();
					consumer.shutdown();
				} catch (InterruptedException e) {
					logger.log(Level.WARNING, "Consumer shutdown interrupted while waiting for active threads to finish.", e);
				}

				logger.info("Shutdown complete.");
			}
		};

		Runtime.getRuntime().addShutdownHook(shutdownHook);

		if (QueueType.NONE == queueType) {
			final List<String> directories = cmd.getArgList();
			final Scanner scanner = new QueueingScanner(logger, queue);

			if (directories.size() == 0) {
				throw new IllegalArgumentException("When not using a queue, you must pass the directory paths to scan on the command line.");
			}

			QueueCli.setScannerOptions(cmd, scanner);
			for (String directory : directories) {
				scanner.scan(Paths.get(directory));
			}
		}

		// Blocks until the queue has drained.
		consumer.start();

		try {

			// Blocks until all the consumer threads have finished, after the queue has drained.
			consumer.awaitTermination();
		} catch (InterruptedException e) {
			logger.warning("Interrupted while waiting for extraction to terminate.");
			Thread.currentThread().interrupt();
		}

		try {
			spewer.finish();
		} catch (IOException e) {
			logger.log(Level.SEVERE, "Spewer failed to finish.", e);
		}

		if (ReporterType.REDIS == reporterType || QueueType.REDIS == queueType) {
			getRedisson(cmd).shutdown();
		}

		Runtime.getRuntime().removeShutdownHook(shutdownHook);

		return cmd;
	}

	public void printHelp() {
		super.printHelp(Command.SPEW, "Extract from files.");
	}
}
