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
public class SolrCommitCli extends Cli {

	public SolrCommitCli(Logger logger) {
		super(logger, new SolrOptionSet());

		options.addOption(Option.builder()
			.desc("Performs a soft commit. Makes index changes visible while neither fsync-ing index files nor writing a new index descriptor. This could lead to data loss if Solr is terminated unexpectedly.")
			.longOpt("soft")
			.build());
	}

	public CommandLine parse(String[] args) throws ParseException, RuntimeException {
		final CommandLine cmd = super.parse(args);

		try (
			final CloseableHttpClient httpClient = PinnedHttpClientBuilder.createWithDefaults()
				.setVerifyHostname(cmd.getOptionValue("verify-host"))
				.pinCertificate(cmd.getOptionValue("pin-certificate"))
				.build();
			final SolrClient client = new HttpSolrClient(cmd.getOptionValue('s'), httpClient);
		) {
			if (cmd.hasOption("soft")) {
				client.commit(true, true, true);
			} else {
				client.commit(true, true, false);
			}
		} catch (SolrServerException e) {
			throw new RuntimeException("Unable to commit.", e);
		} catch (IOException e) {
			throw new RuntimeException("There was an error while communicating with Solr.", e);
		}

		return cmd;
	}

	public void printHelp() {
		super.printHelp(Command.SOLR_COMMIT, "Send a hard or soft commit message to Solr.");
	}
}
