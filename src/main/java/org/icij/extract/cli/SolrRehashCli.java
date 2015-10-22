package org.icij.extract.cli;

import org.icij.extract.core.*;
import org.icij.extract.cli.options.*;
import org.icij.extract.solr.*;
import org.icij.extract.http.PinnedHttpClientBuilder;
import org.icij.extract.solr.SolrDefaults;

import java.util.Map;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;

import java.util.logging.Logger;

import java.io.IOException;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;

import hu.ssh.progressbar.ProgressBar;
import hu.ssh.progressbar.console.ConsoleProgressBar;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public class SolrRehashCli extends Cli {

	public SolrRehashCli(Logger logger) {
		super(logger, new SolrOptionSet());

		options.addOption(Option.builder("i")
				.desc(String.format("The name of the unique ID field in the target Solr schema. Defaults to %s.", SolrDefaults.DEFAULT_ID_FIELD))
				.longOpt("id-field")
				.hasArg()
				.argName("name")
				.build())

			.addOption(Option.builder("a")
				.desc("The hashing algorithm used for generating Solr document identifiers e.g. \"MD5\" or \"SHA-256\".")
				.longOpt("id-algorithm")
				.hasArg()
				.argName("name")
				.required(true)
				.build())

			.addOption(Option.builder("f")
				.desc("Filter documents for which to perform rehashing using the specified query.")
				.longOpt("filter")
				.hasArg()
				.argName("query")
				.build())

			.addOption(Option.builder()
				.desc("Replace part of the path using a regex pattern.")
				.longOpt("pattern")
				.hasArg()
				.argName("pattern")
				.build())

			.addOption(Option.builder()
				.desc("Replacement for the path regex. Defaults to an empty string.")
				.longOpt("replacement")
				.hasArg()
				.argName("replacement")
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
		final int parallelism;

		if (cmd.hasOption('p')) {
			parallelism = ((Number) cmd.getParsedOptionValue("p")).intValue();
		} else {
			parallelism = Runtime.getRuntime().availableProcessors();
		}

		final ProgressBar progressBar = ConsoleProgressBar.on(System.out)
			.withFormat("[:bar] :percent% :elapsed/:total ETA: :eta");

		try (
			final CloseableHttpClient httpClient = PinnedHttpClientBuilder.createWithDefaults()
				.setVerifyHostname(cmd.getOptionValue("verify-host"))
				.pinCertificate(cmd.getOptionValue("pin-certificate"))
				.build();
			final SolrClient client = new HttpSolrClient(cmd.getOptionValue('s'), httpClient);
		) {
			final SolrRehashConsumer consumer = new SolrRehashConsumer(logger, client,
				cmd.getOptionValue('a').toUpperCase(Locale.ROOT));
			final SolrMachineProducer producer = new SolrMachineProducer(logger, client,
				new HashSet<String>(Arrays.asList("*")), parallelism);
			final SolrMachine machine =
				new SolrMachine(logger, consumer, producer, parallelism);

			consumer.setProgressBar(progressBar);
			producer.setProgressBar(progressBar);

			if (cmd.hasOption('i')) {
				consumer.setIdField(cmd.getOptionValue('i'));
				producer.setIdField(cmd.getOptionValue('i'));
			}

			if (cmd.hasOption('f')) {
				producer.setFilter(cmd.getOptionValue('f'));
			}

			if (cmd.hasOption("pattern")) {
				consumer.setPattern(cmd.getOptionValue("pattern"));
			}

			if (cmd.hasOption("replacement")) {
				cmd.getOptionValue("replacement");
			}

			final Integer copied = machine.call();
			machine.terminate();
			logger.info(String.format("Rehashed a total of %d documents.", copied));

			if (cmd.hasOption("soft-commit")) {
				client.commit(true, true, true);
			} else if (cmd.hasOption('c')) {
				client.commit(true, true, false);
			}
		} catch (SolrServerException e) {
			throw new RuntimeException("Unable to rehash.", e);
		} catch (IOException e) {
			throw new RuntimeException("Unable to rehash because of an error while communicating with Solr.", e);
		} catch (InterruptedException e) {
			logger.warning("Interrupted.");
		}

		return cmd;
	}

	public void printHelp() {
		super.printHelp(Command.SOLR_REHASH,
			"Recalculate IDs using a path replacement and/or new digest algorithm.");
	}
}
