package org.icij.extract.cli;

import org.icij.extract.core.*;

import java.util.Map;

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
public class ExtractCli extends Cli {

	public ExtractCli(Logger logger) {
		super(logger, new String[] {
			"v", "q", "d", "redis-namespace", "redis-address", "include-pattern", "exclude-pattern", "follow-symlinks", "queue-poll", "p", "ocr-language", "ocr-disabled", "o", "output-encoding", "file-output-directory", "s", "t", "f", "i", "solr-id-algorithm", "solr-commit-interval", "solr-commit-within", "solr-pin-certificate", "solr-verify-host", "r"
		});
	}

	protected Option createOption(String name) {
		switch (name) {

		case "s": return Option.builder("s")
			.desc("Solr server address. Required if outputting to Solr.")
			.longOpt("solr-address")
			.hasArg()
			.argName("address")
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
			spewer = new FileSpewer(logger);

			// TODO: Ensure that the output directory is not the same as the input directory.
			((FileSpewer) spewer).setOutputDirectory(Paths.get((String) cmd.getOptionValue("file-output-directory", ".")));
		} else {
			spewer = new StdOutSpewer(logger);
		}

		final QueueType queueType = QueueType.parse(cmd.getOptionValue('q'));
		final ReporterType reporterType = ReporterType.parse(cmd.getOptionValue('r'));
		final Consumer consumer;

		// With Redis it's a bit more complex.
		// Run all the jobs in the queue and exit without waiting for more.
		if (QueueType.REDIS == queueType) {
			final Redisson redisson = getRedisson(cmd);
			final RQueue<String> queue = redisson.getQueue(cmd.getOptionValue("redis-namespace", "extract") + ":queue");

			logger.info("Setting up polling consumer.");

			consumer = new PollingConsumer(logger, queue, spewer, threads) {

				@Override
				public void finish() throws InterruptedException, ExecutionException {
					super.finish();
					redisson.shutdown();
				}
			};

			if (cmd.hasOption("queue-poll")) {
				((PollingConsumer) consumer).setPollTimeout((String) cmd.getOptionValue("queue-poll"));
			}

		// When running in memory mode, don't use a queue.
		// The scanner sends jobs straight to the consumer, the executor of which uses its own internal queue.
		// Scanning the directory tree will most probably finish before extraction, so after scanning block until the consumer is done (finish).
		} else {
			consumer = new QueueingConsumer(logger, spewer, threads);
		}

		if (cmd.hasOption("output-encoding")) {
			consumer.setOutputEncoding((String) cmd.getOptionValue("output-encoding"));
		}

		if (cmd.hasOption("ocr-language")) {
			consumer.setOcrLanguage((String) cmd.getOptionValue("ocr-language"));
		}

		if (cmd.hasOption("ocr-disabled")) {
			consumer.disableOcr();
		}

		if (ReporterType.REDIS == reporterType) {
			final Redisson redisson = getRedisson(cmd);
			final RMap<String, Integer> report = RedisReporter.getReport(cmd.getOptionValue("redis-namespace"), redisson);
			final Reporter reporter = new RedisReporter(logger, report);

			logger.info("Using Redis reporter.");
			consumer.setReporter(reporter);
		}

		consumer.start();

		if (QueueType.NONE == queueType) {
			final Scanner scanner;
			final String directory;

			scanner = new ConsumingScanner(logger, (QueueingConsumer) consumer);
			directory = (String) cmd.getOptionValue('d', ".");

			QueueCli.setScannerOptions(cmd, scanner);

			scanner.scan(Paths.get(directory));
			logger.info("Completed scanning of \"" + directory + "\".");
		}

		try {
			consumer.finish();
		} catch (InterruptedException e) {
			logger.warning("Interrupted while waiting for extraction to terminate.");
		} catch (ExecutionException e) {
			logger.log(Level.SEVERE, "Extraction failed for a pending job.", e);
		}

		consumer.shutdown();

		try {
			spewer.finish();
		} catch (IOException e) {
			logger.log(Level.SEVERE, "Spewer failed to finish.", e);
		}

		return cmd;
	}

	public void printHelp() {
		super.printHelp(Command.EXTRACT, "Extract from files.");
	}
}
