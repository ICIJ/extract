package org.icij.extract.cli;

import org.icij.extract.core.*;

import java.util.List;
import java.util.Map;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;

import java.io.IOException;

import java.nio.file.Path;
import java.nio.file.Paths;

import java.util.concurrent.ExecutionException;
import java.security.NoSuchAlgorithmException;

import org.redisson.Redisson;
import org.redisson.core.RQueue;
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
			"v", "q", "n", "redis-address", "include-pattern", "exclude-pattern", "follow-symlinks", "queue-poll", "p", "ocr-language", "ocr-disabled", "ocr-timeout", "o", "output-encoding", "output-base", "file-output-directory", "s", "t", "f", "i", "solr-id-algorithm", "solr-commit-interval", "solr-commit-within", "solr-pin-certificate", "solr-verify-host", "r"
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
			.desc("Time to wait when polling the queue e.g. \"5s\". Defaults to " + PollingConsumer.DEFAULT_TIMEOUT + " " + PollingConsumer.DEFAULT_TIMEOUT_UNIT.name().toLowerCase(Locale.ROOT) + ".")
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

		case "ignore-embeds": return Option.builder()
			.desc("Don't extract text from embedded documents.")
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
			.desc("Set the timeout for the Tesseract process to finish. Default is 120s.")
			.longOpt(name)
			.hasArg()
			.argName("duration")
			.type(Number.class)
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

		case "file-output-directory": return Option.builder()
			.desc("Directory to output extracted text. Defaults to the current directory.")
			.longOpt(name)
			.hasArg()
			.argName("path")
			.build();

		case "file-output-extension": return Option.builder()
			.desc("Extension for files containing extracted text. Defaults to " + FileSpewer.DEFAULT_EXTENSION + ".")
			.longOpt(name)
			.hasArg()
			.argName("extension")
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
				final String algorithm = (String) cmd.getOptionValue("solr-id-algorithm", "SHA-256");

				try {
					((SolrSpewer) spewer).setIdField(cmd.getOptionValue('i'), algorithm);
				} catch (NoSuchAlgorithmException e) {
					throw new IllegalArgumentException("Hashing algorithm not available on this platform: " + algorithm + ".");
				}
			}

			if (cmd.hasOption("solr-commit-interval")) {
				((SolrSpewer) spewer).setCommitInterval(((Number) cmd.getParsedOptionValue("solr-commit-interval")).intValue());
			}

			if (cmd.hasOption("solr-commit-within")) {
				((SolrSpewer) spewer).setCommitWithin(((Number) cmd.getParsedOptionValue("solr-commit-within")).intValue());
			}
		} else if (OutputType.FILE == outputType) {
			spewer = new FileSpewer(logger, Paths.get((String) cmd.getOptionValue("file-output-directory", ".")));

			if (cmd.hasOption("file-output-extension")) {
				((FileSpewer) spewer).setOutputExtension(cmd.getOptionValue("file-output-extension"));
			}
		} else {
			spewer = new PrintStreamSpewer(logger, System.out);
		}

		if (cmd.hasOption("output-base")) {
			spewer.setOutputBase(cmd.getOptionValue("output-base"));
		}

		final QueueType queueType = QueueType.parse(cmd.getOptionValue('q'));
		final ReporterType reporterType = ReporterType.parse(cmd.getOptionValue('r'));
		final Extractor extractor = new Extractor(logger);
		final Consumer consumer;

		Redisson redisson = null;

		// With Redis it's a bit more complex.
		// Run all the jobs in the queue and exit without waiting for more.
		if (QueueType.REDIS == queueType) {
			redisson = getRedisson(cmd);

			final RQueue<String> queue = redisson.getQueue(cmd.getOptionValue('n', "extract") + ":queue");

			logger.info("Setting up polling consumer.");

			consumer = new PollingConsumer(logger, queue, spewer, extractor, threads);

			if (cmd.hasOption("queue-poll")) {
				((PollingConsumer) consumer).setPollTimeout((String) cmd.getOptionValue("queue-poll"));
			}

		// When running in memory mode, don't use a queue.
		// The scanner sends jobs straight to the consumer, the executor of which uses its own internal queue.
		// Scanning the directory tree will most probably finish before extraction, so after scanning block until the consumer is done (finish).
		} else {
			consumer = new QueueingConsumer(logger, spewer, extractor, threads);
		}

		if (cmd.hasOption("output-encoding")) {
			consumer.setOutputEncoding(cmd.getOptionValue("output-encoding"));
		}

		if (cmd.hasOption("ocr-language")) {
			extractor.setOcrLanguage(cmd.getOptionValue("ocr-language"));
		}

		if (cmd.hasOption("ocr-timeout")) {
			extractor.setOcrTimeout(((Number) cmd.getParsedOptionValue("ocr-timeout")).intValue());
		}

		if (cmd.hasOption("ocr-disabled")) {
			extractor.disableOcr();
		}

		if (cmd.hasOption("ignore-embeds")) {
			extractor.ignoreEmbeds();
		}

		if (ReporterType.REDIS == reporterType) {
			redisson = getRedisson(cmd);

			final RMap<String, Integer> report = redisson.getMap(cmd.getOptionValue('n', "extract") + ":report");
			final Reporter reporter = new Reporter(logger, report);

			logger.info("Using Redis reporter.");
			consumer.setReporter(reporter);
		}

		final Thread shutdownHook = new Thread() {
			public void run() {
				logger.warning("Shutdown hook triggered. Please wait for the process to finish cleanly.");

				try {
					consumer.shutdown();
				} catch (InterruptedException e) {
					logger.log(Level.WARNING, "Consumer shutdown interrupted while waiting for active threads to finish.", e);
				}

				logger.info("Shutdown complete.");
			}
		};

		Runtime.getRuntime().addShutdownHook(shutdownHook);
		consumer.start();

		if (QueueType.NONE == queueType) {
			final Scanner scanner = new ConsumingScanner(logger, (QueueingConsumer) consumer);
			final List<String> directories = cmd.getArgList();

			if (directories.size() == 0) {
				throw new IllegalArgumentException("When not using a queue, you must pass the directory paths to scan on the command line.");
			}

			QueueCli.setScannerOptions(cmd, scanner);

			for (String directory : directories) {
				scanner.scan(Paths.get(directory));

				logger.info("Completed scanning of \"" + directory + "\".");
			}
		}

		try {
			consumer.finish();
		} catch (InterruptedException e) {
			logger.warning("Interrupted while waiting for extraction to terminate.");
			Thread.currentThread().interrupt();
		} catch (ExecutionException e) {
			logger.log(Level.SEVERE, "Extraction failed for a pending job.", e);
		}

		try {
			spewer.finish();
		} catch (IOException e) {
			logger.log(Level.SEVERE, "Spewer failed to finish.", e);
		}

		if (null != redisson) {
			redisson.shutdown();
		}

		Runtime.getRuntime().removeShutdownHook(shutdownHook);

		return cmd;
	}

	public void printHelp() {
		super.printHelp(Command.SPEW, "Extract from files.");
	}
}
