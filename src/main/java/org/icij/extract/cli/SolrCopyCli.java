package org.icij.extract.cli;

import org.icij.extract.core.*;
import org.icij.extract.cli.options.*;
import org.icij.extract.solr.CopyMachine;
import org.icij.extract.http.PinnedHttpClientBuilder;

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
public class SolrCopyCli extends Cli {

	public SolrCopyCli(Logger logger) {
		super(logger, new SolrOptionSet());

		options.addOption(Option.builder("i")
				.desc(String.format("The name of the unique ID field in the target Solr schema. Defaults to %s.", CopyMachine.DEFAULT_ID_FIELD))
				.longOpt("id-field")
				.hasArg()
				.argName("name")
				.build())

			.addOption(Option.builder("b")
				.desc(String.format("The number of documents to process at a time. Defaults to %d. To improve performance, set to a lower number if the fields contain very large values.", CopyMachine.DEFAULT_BATCH_SIZE))
				.longOpt("batch-size")
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

		final String[] mappings = cmd.getArgs();
		final Map<String, String> map = new HashMap<String, String>();

		if (0 == args.length) {
			throw new IllegalArgumentException("You must pass the field mappings on the command line.");
		}

		for (String mapping : mappings) {
			String[] fields = mapping.split(":", 2);
	
			if (fields.length > 1) {
				map.put(fields[0], fields[1]);
			} else {
				map.put(fields[0], null);
			}
		}

		try (
			final CloseableHttpClient httpClient = PinnedHttpClientBuilder.createWithDefaults()
				.setVerifyHostname(cmd.getOptionValue("verify-host"))
				.pinCertificate(cmd.getOptionValue("pin-certificate"))
				.build();
			final SolrClient client = new HttpSolrClient(cmd.getOptionValue('s'), httpClient);
		) {
			final CopyMachine machine =
				new CopyMachine(logger, client, map);

			if (cmd.hasOption('i')) {
				machine.setIdField(cmd.getOptionValue('i'));
			}

			if (cmd.hasOption('b')) {
				machine.setBatchSize(((Number) cmd.getParsedOptionValue("b")).intValue());
			}

			final int copied = machine.copy();
			logger.info(String.format("Copied a total of %d documents.", copied));

			if (cmd.hasOption("soft-commit")) {
				client.commit(true, true, true);
			} else if (cmd.hasOption('c')) {
				client.commit(true, true, false);
			}
		} catch (SolrServerException e) {
			throw new RuntimeException("Unable to copy.", e);
		} catch (IOException e) {
			throw new RuntimeException("Unable to copy because of an error while communicating with Solr.", e);
		}

		return cmd;
	}

	public void printHelp() {
		super.printHelp(Command.SOLR_COPY, "Copy Solr fields from one field to another, or back to the same field to force reindexing.");
	}
}
