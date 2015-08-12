package org.icij.extract.cli;

import org.icij.extract.core.*;
import org.icij.extract.cli.options.*;
import org.icij.extract.solr.*;
import org.icij.extract.http.PinnedHttpClientBuilder;
import org.icij.extract.solr.SolrDefaults;

import java.util.Map;
import java.util.List;
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

		options.addOption(Option.builder("m")
				.desc("The mode. Either \"intersection\" or \"complement\".")
				.longOpt("mode")
				.hasArg()
				.argName("mode")
				.required(true)
				.build())

			.addOption(Option.builder("i")
				.desc(String.format("The name of the unique ID field in the target Solr schema. Defaults to %s.", SolrDefaults.DEFAULT_ID_FIELD))
				.longOpt("id-field")
				.hasArg()
				.argName("name")
				.build())

			.addOption(Option.builder("a")
				.desc(String.format("Address of the first Solr core in the set. This should be the smaller of the two cores, as iteration will occur over this set."))
				.longOpt("a-address")
				.hasArg()
				.argName("address")
				.required(true)
				.build())

			.addOption(Option.builder("b")
				.desc(String.format("Address of the second Solr core in the set. This should be the larger of the two cores."))
				.longOpt("b-address")
				.hasArg()
				.argName("address")
				.required(true)
				.build())

			.addOption(Option.builder("p")
				.desc(String.format("The number of documents to process at a time. Defaults to the number of available processors. To improve performance, set to a lower number if the fields contain very large values."))
				.longOpt("parallel")
				.hasArg()
				.argName("size")
				.type(Number.class)
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

		if (0 == args.length) {
			throw new IllegalArgumentException("You must pass literals on the command line.");
		}

		final Map<String, String> literals = new HashMap<String, String>();

		for (String literal : cmd.getArgs()) {
			String[] pair = literal.split(":", 2);

			if (2 == pair.length) {
				literals.put(pair[0], pair[1]);
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

		try (
			final CloseableHttpClient httpClient = PinnedHttpClientBuilder.createWithDefaults()
				.setVerifyHostname(cmd.getOptionValue("verify-host"))
				.pinCertificate(cmd.getOptionValue("pin-certificate"))
				.build();
			final SolrClient a = new HttpSolrClient(cmd.getOptionValue('a'), httpClient);
			final SolrClient b = new HttpSolrClient(cmd.getOptionValue('b'), httpClient);
			final SolrClient destination = new HttpSolrClient(cmd.getOptionValue('s'), httpClient);
		) {
			final SolrMachineConsumer consumer;

			if (cmd.getOptionValue('m').equals("intersection")) {
				consumer = new SolrIntersectionConsumer(logger, b, destination, literals);
			} else if (cmd.getOptionValue('m').equals("complement")) {
				consumer = new SolrComplementConsumer(logger, b, destination, literals);
			} else {
				throw new IllegalArgumentException(String.format("Invalid mode: ", cmd.getOptionValue('m')));
			}

			final SolrMachineProducer producer = new SolrMachineProducer(logger, a, null, parallelism);
			final SolrMachine machine =
				new SolrMachine(logger, consumer, producer, parallelism);

			if (cmd.hasOption('i')) {
				consumer.setIdField(cmd.getOptionValue('i'));
				producer.setIdField(cmd.getOptionValue('i'));
			}

			final Integer processed = machine.call();
			machine.terminate();
			logger.info(String.format("Processed a total of %d documents.", processed));
			logger.info(String.format("Tagged %d documents in subset.", consumer.getConsumeCount()));

			if (cmd.hasOption("soft-commit")) {
				destination.commit(true, true, true);
			} else if (cmd.hasOption('c')) {
				destination.commit(true, true, false);
			}
		} catch (SolrServerException e) {
			throw new RuntimeException("Unable to copy.", e);
		} catch (IOException e) {
			throw new RuntimeException("Unable to tag because of an error while communicating with Solr.", e);
		} catch (InterruptedException e) {
			logger.warning("Interrupted.");
		}

		return cmd;
	}

	public void printHelp() {
		super.printHelp(Command.SOLR_TAG,
			"Tag the intersect or complement of two Solr cores.\n\n" +
			"An intersect subset is calculated by iterating over documents in the core specified by the " +
			"\033[1m-a\033[0m option and checking whether they exist in the core specified by the " +
			"\033[1m-b\033[0m option.\n\n" +
			"A complement subset consists of those documents in the core specified by the \033[1m-a\033[0m option " +
			"which are not present in the core specified by the \033[1m-b\033[0m option.\n\n" +
			"Use literals to tag the documents in the core specified by the \033[1m-s\033[0m option, for example, \"batch:1\".",
			"literals...");
	}
}
