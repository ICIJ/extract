package org.icij.extract.cli;

import org.icij.extract.core.*;
import org.icij.extract.cli.options.*;
import org.icij.extract.http.PinnedHttpClientBuilder;

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
				.desc("The name of the unique ID field in the target Solr schema.")
				.longOpt("id-field")
				.hasArg()
				.argName("name")
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

		final String idField = cmd.getOptionValue('i', SolrSpewer.DEFAULT_ID_FIELD);
		final String[] ids = cmd.getArgs();

		if (0 == ids.length) {
			throw new IllegalArgumentException("You must pass the IDs to delete on the command line.");
		}

		try (
			final CloseableHttpClient httpClient = PinnedHttpClientBuilder.createWithDefaults()
				.setVerifyHostname(cmd.getOptionValue("verify-host"))
				.pinCertificate(cmd.getOptionValue("pin-certificate"))
				.build();
			final SolrClient client = new HttpSolrClient(cmd.getOptionValue('s'), httpClient);
		) {
			for (String id : ids) {
				if (id.contains("*")) {
					logger.info(String.format("Deleting document matching ID pattern %s.", id));
					client.deleteByQuery(idField + ":" + id);
				} else {
					logger.info(String.format("Deleting document with ID %s.", id));
					client.deleteById(id);
				}
			}

			if (cmd.hasOption('c')) {
				if (cmd.hasOption("soft-commit")) {
					client.commit(true, true, true);
				} else {
					client.commit(true, true, false);
				}
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
