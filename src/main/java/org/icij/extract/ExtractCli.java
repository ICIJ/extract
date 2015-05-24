package org.icij.extract;

import java.util.logging.Level;
import java.util.logging.Logger;

import java.io.IOException;

import java.nio.file.Path;
import java.nio.file.Paths;

import java.util.concurrent.ExecutionException;

import org.redisson.Redisson;
import org.redisson.core.RQueue;

import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.CommandLine;

import org.apache.solr.client.solrj.impl.HttpSolrClient;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public class ExtractCli extends Cli {

	public static void setConsumerOptions(CommandLine cli, Consumer consumer) {
		if (cli.hasOption("output-encoding")) {
			consumer.setOutputEncoding((String) cli.getOptionValue("output-encoding"));
		}

		if ("auto".equals(((String) cli.getOptionValue("ocr-language")))) {
			consumer.detectLanguageForOcr();
		} else if (cli.hasOption("ocr-language")) {
			consumer.setOcrLanguage((String) cli.getOptionValue("ocr-language"));
		}

		if (cli.hasOption("queue-poll") && consumer instanceof PollingConsumer) {
			((PollingConsumer) consumer).setPollTimeout((String) cli.getOptionValue("queue-poll"));
		}
	}

	public ExtractCli(Logger logger) {
		super(logger);
	}

	public CommandLine parse(String[] args) throws ParseException, IllegalArgumentException {
		final CommandLine cli = super.parse(args, Command.EXTRACT);

		int threads = Consumer.DEFAULT_THREADS;

		if (cli.hasOption('p')) {
			try {
				threads = ((Number) cli.getParsedOptionValue("t")).intValue();
			} catch (ParseException e) {
				throw new IllegalArgumentException("Invalid value for thread count.");
			}
		}

		logger.info("Processing up to " + threads + " file(s) in parallel.");

		final OutputType outputType;
		final Spewer spewer;

		try {
			outputType = OutputType.fromString(cli.getOptionValue('o', "stdout"));
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException(String.format("\"%s\" is not a valid output type.", cli.getOptionValue('o')));
		}

		if (OutputType.SOLR == outputType) {
			if (!cli.hasOption('s')) {
				throw new IllegalArgumentException("The -s option is required when outputting to Solr.");
			}

			spewer = new SolrSpewer(logger, new HttpSolrClient(cli.getOptionValue('s')));
			if (cli.hasOption('t')) {
				((SolrSpewer) spewer).setField(cli.getOptionValue('t'));
			}

			if (cli.hasOption("solr-commit-interval")) {
				((SolrSpewer) spewer).setCommitInterval(((Number) cli.getParsedOptionValue("solr-commit-interval")).intValue());
			}
		} else if (OutputType.FILE == outputType) {
			spewer = new FileSpewer(logger);

			// TODO: Ensure that the output directory is not the same as the input directory.
			((FileSpewer) spewer).setOutputDirectory(Paths.get((String) cli.getOptionValue("file-output-directory", ".")));
		} else {
			spewer = new StdOutSpewer(logger);
		}

		final QueueType queueType = QueueType.parse(cli.getOptionValue('q', "memory"));

		if (QueueType.REDIS == queueType) {
			final Redisson redisson = createRedisClient(cli);
			final RQueue<Path> queue = createRedisQueue(cli, redisson);

			// With Redis it's a bit more complex.
			// Run all the jobs in the queue and exit without waiting for more.
			final PollingConsumer consumer = new PollingConsumer(logger, queue, spewer, threads) {

				@Override
				protected void drained() {
					super.drained();

					try {
						finish();
					} catch (InterruptedException e) {
						logger.warning("Interrupted while waiting for extraction to terminate.");
					} catch (ExecutionException e) {
						logger.log(Level.SEVERE, "Extraction failed for a pending job.", e);
					}

					try {
						spewer.finish();
					} catch (IOException e) {
						logger.log(Level.SEVERE, "Spewer failed to finish.", e);
					}

					shutdown();
					redisson.shutdown();
				}
			};

			setConsumerOptions(cli, consumer);
			consumer.saturate();
		} else {
			final Scanner scanner;
			final String directory;
			final Consumer consumer;

			// When running in memory mode, don't use a queue.
			// The scanner sends jobs straight to the consumer, the executor of which uses its own internal queue.
			// Scanning the directory tree will most probably finish before extraction, so after scanning block until the consumer is done (finish).
			consumer = new QueueingConsumer(logger, spewer, threads);
			scanner = new ConsumingScanner(logger, (QueueingConsumer) consumer);
			directory = (String) cli.getOptionValue('d', "*");

			QueueCli.setScannerOptions(cli, scanner);
			setConsumerOptions(cli, consumer);

			scanner.scan(Paths.get(directory));
			logger.info("Completed scanning of \"" + directory + "\".");

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
		}

		return cli;
	}

	public void printHelp() {
		super.printHelp(Command.EXTRACT, "Extract from files.");
	}
}
