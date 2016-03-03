package org.icij.extract.cli;

import hu.ssh.progressbar.ProgressBar;
import hu.ssh.progressbar.console.ConsoleProgressBar;
import org.icij.extract.cli.options.*;
import org.icij.extract.solr.*;
import org.icij.extract.http.PinnedHttpClientBuilder;

import java.util.Map;
import java.util.HashMap;

import java.util.logging.Logger;

import java.io.IOException;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public class SolrTagCli extends Cli {

	public SolrTagCli(Logger logger) {
		super(logger, new SolrOptionSet());

		options.addOption(Option.builder("f")
				.desc("Filter documents to tag using the specified query.")
				.longOpt("filter")
				.hasArg()
				.argName("query")
				.build())

			.addOption(Option.builder("m")
				.desc("The mode to use if calculating a subset of two cores. When specified, either \"intersection\" or \"complement\".")
				.longOpt("subset-mode")
				.hasArg()
				.argName("mode")
				.build())

			.addOption(Option.builder("i")
				.desc(String.format("The name of the unique ID field in the target Solr schema. Defaults to %s.", SolrDefaults.DEFAULT_ID_FIELD))
				.longOpt("id-field")
				.hasArg()
				.argName("name")
				.build())

			.addOption(Option.builder("a")
				.desc("Address of the first Solr core in the set. This should be the smaller of the two cores, as iteration will occur over this set.")
				.longOpt("a-address")
				.hasArg()
				.argName("address")
				.build())

			.addOption(Option.builder("b")
				.desc("Address of the second Solr core in the set. This should be the larger of the two cores.")
				.longOpt("b-address")
				.hasArg()
				.argName("address")
				.build())

			.addOption(Option.builder("p")
				.desc("The number of documents to process at a time. Defaults to the number of available processors. To improve performance, set to a lower number if the fields contain very large values.")
				.longOpt("parallel")
				.hasArg()
				.argName("size")
				.type(Number.class)
				.build())

			.addOption(Option.builder()
				.desc("Don't show a progress bar.")
				.longOpt("no-progress")
				.build())

			.addOption(Option.builder("c")
				.desc("Perform a commit when done.")
				.longOpt("commit")
				.build())

			.addOption(Option.builder()
				.desc("Modifies the commit option so that a soft commit is performed instead of a hard commit. Makes index changes visible while neither fsync-ing index files nor writing a new index descriptor. This could lead to data loss if Solr is terminated unexpectedly.")
				.longOpt("soft-commit")
				.build());
	}

	public CommandLine parse(String[] args) throws ParseException, RuntimeException {
		final CommandLine cmd = super.parse(args);

		final Map<String, String> pairs = new HashMap<>();
		final String[] literals = cmd.getArgs();

		if (0 == literals.length) {
			throw new IllegalArgumentException("You must pass literals on the command line.");
		}

		for (String literal : literals) {
			String[] pair = literal.split(":", 2);

			if (2 == pair.length) {
				pairs.put(pair[0], pair[1]);
			} else {
				throw new IllegalArgumentException(String.format("Invalid literal pair: %s.", literal));
			}
		}

		final int parallelism;

		if (cmd.hasOption('p')) {
			parallelism = ((Number) cmd.getParsedOptionValue("p")).intValue();
		} else {
			parallelism = Runtime.getRuntime().availableProcessors();
		}

		final String subsetMode = cmd.getOptionValue('m');

		if (null != subsetMode && !(subsetMode.equals("intersection") || subsetMode.equals("complement"))) {
			throw new IllegalArgumentException(String.format("Invalid mode: %s.", subsetMode));
		}

		final String addressA = cmd.getOptionValue('a');
		final String addressB = cmd.getOptionValue('b');

		if (null != subsetMode && (null == addressA || null == addressB)) {
			throw new IllegalArgumentException("Both cores of the set must be specified if operating " +
				"on an intersection or complement.");
		}

		try (
			final CloseableHttpClient httpClient = PinnedHttpClientBuilder.createWithDefaults()
				.setVerifyHostname(cmd.getOptionValue("verify-host"))
				.pinCertificate(cmd.getOptionValue("pin-certificate"))
				.build();
			final SolrClient client = new HttpSolrClient(cmd.getOptionValue('s'), httpClient)
		) {
			final SolrMachineConsumer consumer;
			final SolrMachineProducer producer;
			final Integer processed;

			if (null != subsetMode) {
				try (
					final SolrClient clientA = new HttpSolrClient(addressA, httpClient);
					final SolrClient clientB = new HttpSolrClient(addressB, httpClient)
				) {
					producer = new SolrMachineProducer(logger, clientA, null, parallelism);

					if (subsetMode.equals("intersection")) {
						consumer = new SolrIntersectionConsumer(logger, clientB, client, pairs);
					} else {
						consumer = new SolrComplementConsumer(logger, clientB, client, pairs);
					}

					if (cmd.hasOption('i')) {
						consumer.setIdField(cmd.getOptionValue('i'));
						producer.setIdField(cmd.getOptionValue('i'));
					}

					if (cmd.hasOption('f')) {
						producer.setFilter(cmd.getOptionValue('f'));
					}

					final SolrMachine machine = new SolrMachine(logger, consumer, producer, parallelism);

					if (!cmd.hasOption("no-progress")) {
						final ProgressBar progressBar = ConsoleProgressBar.on(System.out)
								.withFormat("[:bar] :percent% :elapsed/:total ETA: :eta");

						consumer.setProgressBar(progressBar);
						producer.setProgressBar(progressBar);
					}

					processed = machine.call();
					machine.terminate();
				}
			} else {
				consumer = new SolrTaggingConsumer(logger, client, pairs);
				producer = new SolrMachineProducer(logger, client, null, parallelism);

				if (cmd.hasOption('i')) {
					consumer.setIdField(cmd.getOptionValue('i'));
					producer.setIdField(cmd.getOptionValue('i'));
				}

				if (cmd.hasOption('f')) {
					producer.setFilter(cmd.getOptionValue('f'));
				}

				final SolrMachine machine = new SolrMachine(logger, consumer, producer, parallelism);

				if (!cmd.hasOption("no-progress")) {
					final ProgressBar progressBar = ConsoleProgressBar.on(System.out)
							.withFormat("[:bar] :percent% :elapsed/:total ETA: :eta");

					consumer.setProgressBar(progressBar);
					producer.setProgressBar(progressBar);
				}

				processed = machine.call();
				machine.terminate();
			}

			logger.info(String.format("Processed a total of %d documents.", processed));
			logger.info(String.format("Tagged %d documents.", consumer.getConsumeCount()));

			if (cmd.hasOption("soft-commit")) {
				client.commit(true, true, true);
			} else if (cmd.hasOption('c')) {
				client.commit(true, true, false);
			}
		} catch (SolrServerException e) {
			throw new RuntimeException("Unable to tag.", e);
		} catch (IOException e) {
			throw new RuntimeException("Unable to tag because of an error while communicating with Solr.", e);
		} catch (InterruptedException e) {
			logger.warning("Interrupted.");
		}

		return cmd;
	}

	public void printHelp() {
		super.printHelp(Command.SOLR_TAG,
			"Tag the intersect or complement of two Solr cores, or a single core.\n\n" +
			"An intersect subset is calculated by iterating over documents in the core specified by the " +
			"\033[1m-a\033[0m option and checking whether they exist in the core specified by the " +
			"\033[1m-b\033[0m option.\n\n" +
			"A complement subset consists of those documents in the core specified by the \033[1m-a\033[0m option " +
			"which are not present in the core specified by the \033[1m-b\033[0m option.\n\n" +
			"Use literals to tag the documents in the core specified by the \033[1m-s\033[0m option, for example, \"batch:1\".",
			"literals...");
	}
}
