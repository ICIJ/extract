package org.icij.extract.cli;

import org.icij.extract.core.*;
import org.icij.extract.cli.options.*;
import org.icij.extract.cli.factory.QueueFactory;
import org.icij.extract.cli.factory.ReportFactory;
import org.icij.extract.solr.SolrSpewer;
import org.icij.extract.http.PinnedHttpClientBuilder;

import java.util.logging.Level;
import java.util.logging.Logger;

import java.io.IOException;

import java.nio.file.Paths;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.CommandLine;

import org.apache.commons.io.FileUtils;

import org.apache.http.impl.client.CloseableHttpClient;

import org.apache.solr.client.solrj.impl.HttpSolrClient;

import java.lang.management.ManagementFactory;
import com.sun.management.OperatingSystemMXBean;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public class SpewCli extends Cli {

	public SpewCli(final Logger logger) {
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
		final OperatingSystemMXBean os = (com.sun.management.OperatingSystemMXBean)
			ManagementFactory.getOperatingSystemMXBean();
		final long maxMemory = Runtime.getRuntime().maxMemory();

		if (maxMemory < (os.getTotalPhysicalMemorySize() / 4)) {
			logger.warning(String.format("Memory available to JVM (%s) is less than 25%% of available system memory. " +
				"You should probably increase it.", FileUtils.byteCountToDisplaySize(maxMemory)));
		}

		final CommandLine cmd = super.parse(args);
		final int parallelism;

		if (cmd.hasOption('p')) {
			try {
				parallelism = ((Number) cmd.getParsedOptionValue("p")).intValue();
			} catch (ParseException e) {
				throw new IllegalArgumentException("Invalid value for thread count.");
			}
		} else {
			parallelism = Consumer.DEFAULT_PARALLELISM;
		}

		logger.info(String.format("Processing up to %d file(s) in parallel.", parallelism));

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
			spewer = new FileSpewer(logger, Paths.get(cmd.getOptionValue("file-output-directory", ".")));
		} else {
			spewer = new PrintStreamSpewer(logger, System.out);
		}

		SpewerOptionSet.configureSpewer(cmd, spewer);

		final Extractor extractor = new Extractor(logger);
		final Queue queue = QueueFactory.createQueue(cmd);
		final Report report = ReportFactory.createReport(cmd);
		final PollingConsumer consumer = new PollingConsumer(logger, queue, spewer, extractor, parallelism);

		ConsumerOptionSet.configureConsumer(cmd, consumer);
		ExtractorOptionSet.configureExtractor(cmd, extractor);

		if (OutputType.FILE == outputType &&
			extractor.getOutputFormat() == Extractor.OutputFormat.HTML) {
			((FileSpewer) spewer).setOutputExtension("html");
		}

		if (null != report) {
			logger.info("Using reporter.");
			consumer.setReporter(new Reporter(report));
		}

		// Allow parallel and consuming and scanning.
		final Scanner scanner;
		final String[] directories = cmd.getArgs();

		if (directories.length > 0) {
			scanner = new Scanner(logger, queue);

			ScannerOptionSet.configureScanner(cmd, scanner);
			for (String directory : directories) {
				scanner.scan(Paths.get(directory));
			}
		} else {
			scanner = null;
		}

		// Start the consumer before the scanner finishes, so that both run in parallel.
		// But keep polling, without a timeout i.e. wait indefinitely, until the scanner
		// finishes, to mitigate scanner latency.
		if (null != scanner) {
			consumer.drainContinuously();
		}

		final Thread shutdownHook = new Thread() {

			@Override
			public void run() {
				logger.warning("Shutdown hook triggered. Please wait for a clean exit.");

				try {
					if (null != scanner) {
						scanner.stop();
						scanner.finish();
					}

					consumer.stop();
					consumer.finish();
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

			// Block until every single path has been scanned and queued.
			if (null != scanner) {
				scanner.finish();
			}

			// Stop the continuous drain.
			// The subsequent blocking drain will finish off.
			if (null != scanner) {
				consumer.stop();
			}

			// Block until every path in the queue has been consumed.
			consumer.drain(); // Blocking.
			consumer.finish();
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

		try {
			queue.close();
		} catch (IOException e) {
			logger.log(Level.SEVERE, "Error while closing queue.", e);
		}

		if (null != report) {
			try {
				report.close();
			} catch (IOException e) {
				logger.log(Level.SEVERE, "Error while closing report.", e);
			}
		}

		try {
			Runtime.getRuntime().removeShutdownHook(shutdownHook);
		} catch (IllegalStateException e) {
			logger.info("Could not remove shutdown hook.");
		}

		return cmd;
	}

	public void printHelp() {
		super.printHelp(Command.SPEW, "Extract from files.", "[paths...]");
	}
}
