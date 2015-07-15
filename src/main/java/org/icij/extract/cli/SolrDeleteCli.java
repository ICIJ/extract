package org.icij.extract.cli;

import org.icij.extract.core.*;
import org.icij.extract.cli.options.*;
import org.icij.extract.http.PinnedHttpClientBuilder;
import org.icij.extract.solr.SolrDefaults;

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
public class SolrDeleteCli extends Cli {

	public SolrDeleteCli(Logger logger) {
		super(logger, new SolrOptionSet());

		options.addOption(Option.builder("i")
				.desc(String.format("The name of the unique ID field in the target Solr schema. Defaults to %s.", SolrDefaults.DEFAULT_ID_FIELD))
				.longOpt("id-field")
				.hasArg()
				.argName("name")
				.build())

			.addOption(Option.builder("c")
				.desc("Perform a commit when done.")
				.longOpt("commit")
				.build())

			.addOption(Option.builder()
				.desc("Perform a soft commit when done.")
				.longOpt("soft-commit")
				.build());
	}

	public CommandLine parse(String[] args) throws ParseException, RuntimeException {
		final CommandLine cmd = super.parse(args);

		final String[] queries = cmd.getArgs();

		if (0 == queries.length) {
			throw new IllegalArgumentException("You must pass the queries or IDs to delete on the command line.");
		}

		try (
			final CloseableHttpClient httpClient = PinnedHttpClientBuilder.createWithDefaults()
				.setVerifyHostname(cmd.getOptionValue("verify-host"))
				.pinCertificate(cmd.getOptionValue("pin-certificate"))
				.build();
			final SolrClient client = new HttpSolrClient(cmd.getOptionValue('s'), httpClient);
		) {
			for (String query : queries) {
				if (query.contains(":")) {
					logger.info(String.format("Deleting documents matching pattern %s.", query));
					client.deleteByQuery(query);
				} else {
					logger.info(String.format("Deleting document with ID %s.", query));
					client.deleteById(query);
				}
			}

			if (cmd.hasOption("soft-commit")) {
				client.commit(true, true, true);
			} else if (cmd.hasOption('c')) {
				client.commit(true, true, false);
			}
		} catch (SolrServerException e) {
			throw new RuntimeException("Unable to delete.", e);
		} catch (IOException e) {
			throw new RuntimeException("Unable to delete because of an error while communicating with Solr.", e);
		}

		return cmd;
	}

	public void printHelp() {
		super.printHelp(Command.SOLR_DELETE, "Delete documents from Solr.");
	}
}
