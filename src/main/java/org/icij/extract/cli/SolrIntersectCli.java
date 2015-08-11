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
public class SolrIntersectCli extends Cli {

	public SolrIntersectCli(Logger logger) {
		super(logger, new SolrOptionSet());

		options.addOption(Option.builder("i")
				.desc(String.format("The name of the unique ID field in the target Solr schema. Defaults to %s.", SolrDefaults.DEFAULT_ID_FIELD))
				.longOpt("id-field")
				.hasArg()
				.argName("name")
				.build())

			.addOption(Option.builder("a")
				.desc(String.format("Address of the first Solr core in the set."))
				.longOpt("a-address")
				.hasArg()
				.argName("address")
				.required(true)
				.build())

			.addOption(Option.builder("b")
				.desc(String.format("Address of the second Solr core in the set."))
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
			final SolrClient c = new HttpSolrClient(cmd.getOptionValue('s'), httpClient);
		) {
			final SolrMachineConsumer consumer = new SolrIntersectionConsumer(logger, a, b, c, literals);
			final SolrMachineProducer producer = new SolrMachineProducer(logger, a, null, parallelism);
			final SolrMachine machine =
				new SolrMachine(logger, consumer, producer, parallelism);

			if (cmd.hasOption('i')) {
				consumer.setIdField(cmd.getOptionValue('i'));
				producer.setIdField(cmd.getOptionValue('i'));
			}

			final Integer copied = machine.call();
			machine.terminate();
			logger.info(String.format("Copied a total of %d documents.", copied));
			logger.info(String.format("%d documents in intersection.", consumer.getConsumeCount()));

			if (cmd.hasOption("soft-commit")) {
				c.commit(true, true, true);
			} else if (cmd.hasOption('c')) {
				c.commit(true, true, false);
			}
		} catch (SolrServerException e) {
			throw new RuntimeException("Unable to copy.", e);
		} catch (IOException e) {
			throw new RuntimeException("Unable to copy because of an error while communicating with Solr.", e);
		} catch (InterruptedException e) {
			logger.warning("Interrupted.");
		}

		return cmd;
	}

	public void printHelp() {
		super.printHelp(Command.SOLR_INTERSECT,
			"Tag the intersect of two Solr cores.\n\n" +
			"User literals to tag the documents in core B, for example, \"in_old:true\".",
			"literals...");
	}
}
