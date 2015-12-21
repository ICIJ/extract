package org.icij.extract.cli;

import org.icij.extract.cli.options.*;
import org.icij.extract.http.PinnedHttpClientBuilder;

import java.util.logging.Logger;

import java.io.IOException;

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
public class SolrRollbackCli extends Cli {

	public SolrRollbackCli(Logger logger) {
		super(logger, new SolrOptionSet());
	}

	public CommandLine parse(String[] args) throws ParseException, RuntimeException {
		final CommandLine cmd = super.parse(args);

		try (
			final CloseableHttpClient httpClient = PinnedHttpClientBuilder.createWithDefaults()
				.setVerifyHostname(cmd.getOptionValue("verify-host"))
				.pinCertificate(cmd.getOptionValue("pin-certificate"))
				.build();
			final SolrClient client = new HttpSolrClient(cmd.getOptionValue('s'), httpClient)
		) {
			client.rollback();
		} catch (SolrServerException e) {
			throw new RuntimeException("Unable to roll back uncommitted documents.", e);
		} catch (IOException e) {
			throw new RuntimeException("There was an error while communicating with Solr.", e);
		}

		return cmd;
	}

	public void printHelp() {
		super.printHelp(Command.SOLR_ROLLBACK, "Send a rollback message to Solr.");
	}
}
