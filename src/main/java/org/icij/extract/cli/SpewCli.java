package org.icij.extract.cli;

import org.icij.extract.core.*;
import org.icij.extract.cli.options.*;
import org.icij.extract.http.PinnedHttpClientBuilder;

import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import java.io.IOException;

import java.nio.file.Path;
import java.nio.file.Paths;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;

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
		super(logger, new QueueOptionSet(), new ReporterOptionSet(), new RedisOptionSet(),
			new ScannerOptionSet(), new ConsumerOptionSet(), new ExtractorOptionSet(),
			new SpewerOptionSet(), new SolrSpewerOptionSet(), new FileSpewerOptionSet());

		options.addOption(Option.builder("p")
				.desc("The number of files which are processed concurrently. Defaults to the number of available processors.")
				.longOpt("parallel")
				.hasArg()
				.argName("count")
				.type(Number.class)
				.build())

			.addOption(Option.builder("o")
				.desc("Set the output type. Either \"file\", \"stdout\" or \"solr\".")
				.longOpt("output")
				.hasArg()
				.argName("type")
				.build());
	}

	public CommandLine parse(String[] args) throws ParseException, IllegalArgumentException {
		final CommandLine cmd = super.parse(args);

		int parallelism = Consumer.DEFAULT_PARALLELISM;

		if (cmd.hasOption('p')) {
			try {
				parallelism = ((Number) cmd.getParsedOptionValue("p")).intValue();
			} catch (ParseException e) {
				throw new IllegalArgumentException("Invalid value for thread count.");
			}
		}

		logger.info("Processing up to " + parallelism + " file(s) in parallel.");

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

			// Calling #finish on the SolrSpewer later on automatically closes these clients.
			final CloseableHttpClient httpClient = PinnedHttpClientBuilder.createWithDefaults()
				.setVerifyHostname(cmd.getOptionValue("solr-verify-host"))
				.pinCertificate(cmd.getOptionValue("solr-pin-certificate"))
				.build();

			spewer = new SolrSpewer(logger, new HttpSolrClient(cmd.getOptionValue('s'), httpClient));
			SolrSpewerOptionSet.configureSpewer(cmd, (SolrSpewer) spewer);

		} else if (OutputType.FILE == outputType) {
			spewer = new FileSpewer(logger, Paths.get((String) cmd.getOptionValue("file-output-directory", ".")));
		} else {
			spewer = new PrintStreamSpewer(logger, System.out);
		}

		SpewerOptionSet.configureSpewer(cmd, spewer);

		final QueueType queueType = QueueType.parse(cmd.getOptionValue('q'));
		final ReporterType reporterType = ReporterType.parse(cmd.getOptionValue('r'));
		final Extractor extractor = new Extractor(logger);

		final BlockingQueue<String> queue;

		if (QueueType.REDIS == queueType) {
			queue = getRedisson(cmd).getBlockingQueue(cmd.getOptionValue("queue-name", "extract") + ":queue");
		} else {

			// Create a classic "bounded buffer", in which a fixed-sized array holds
			// elements inserted by producers and extracted by consumers.
			// The scanner will pause every time the bound is hit. This prevents it
			// from quickly using up memory with a massive file list.
			queue = new ArrayBlockingQueue<String>(parallelism * 2);
		}

		final PollingConsumer consumer = new PollingConsumer(logger, queue, spewer, extractor, parallelism);

		ConsumerOptionSet.configureConsumer(cmd, consumer);
		ExtractorOptionSet.configureExtractor(cmd, extractor);

		if (OutputType.FILE == outputType &&
			extractor.getOutputFormat() == Extractor.OutputFormat.HTML) {

			((FileSpewer) spewer).setOutputExtension("html");
		}

		if (ReporterType.REDIS == reporterType) {
			final RMap<String, Integer> report = getRedisson(cmd).getMap(cmd.getOptionValue("report-name", "extract") + ":report");
			final Reporter reporter = new Reporter(logger, report);

			logger.info("Using Redis reporter.");
			consumer.setReporter(reporter);
		}

		// Allow parallel and consuming and scanning, even when the queue is Redis.
		final Scanner scanner = new QueueingScanner(logger, queue);
		final String[] directories = cmd.getArgs();

		if (directories.length == 0 && QueueType.NONE == queueType) {
			throw new IllegalArgumentException("When not using a queue, you must pass the directory " +
				"paths to scan on the command line.");
		}

		ScannerOptionSet.configureScanner(cmd, scanner);
		for (String directory : directories) {
			scanner.scan(Paths.get(directory));
		}

		final Thread shutdownHook = new Thread() {

			@Override
			public void run() {
				logger.warning("Shutdown hook triggered. Please wait for a clean exit.");

				// The scanner may be stopped forcibly but the the consumer
				// should be allowed to exit cleanly.
				scanner.shutdownNow();
				consumer.stop();
				consumer.shutdown();

				try {
					consumer.awaitTermination();
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					logger.warning("Exiting forcefully.");
					return;
				}

				try {
					spewer.finish();
				} catch (IOException e) {
					logger.log(Level.SEVERE, "Spewer failed to finish.", e);
				}

				logger.info("Shutdown complete.");
			}
		};

		Runtime.getRuntime().addShutdownHook(shutdownHook);
		try {

			// Blocks until the queue has drained by the consumer.
			// Start the consumer before the scanner finishes, so that both run in parallel.
			consumer.start();

			// Block until every single path has been scanned and queued.
			scanner.awaitTermination();
			scanner.shutdown();

			// Block until every path in the queue has been consumed.
			consumer.awaitTermination();
			consumer.shutdown();
		} catch (InterruptedException e) {

			// Exit early and let the shutdown hook make a clean exit.
			logger.warning("Interrupted.");
			Thread.currentThread().interrupt();
			return cmd;
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
		super.printHelp(Command.SPEW, "Extract from files.", "[paths...]");
	}
}
