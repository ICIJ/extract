package org.icij.extract.cli;

import org.icij.extract.core.*;

import java.util.logging.Logger;

import java.io.IOException;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;

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
		super(logger, new String[] {
			"v", "s", "solr-pin-certificate"
		});
	}

	protected Option createOption(String name) {
		switch (name) {

		case "s": return Option.builder("s")
			.desc("Solr server address. Required.")
			.longOpt("solr-address")
			.hasArg()
			.argName("address")
			.required(true)
			.build();

		default:
			return super.createOption(name);
		}
	}

	public CommandLine parse(String[] args) throws ParseException, RuntimeException {
		final CommandLine cmd = super.parse(args);

		if (cmd.hasOption("solr-pin-certificate")) {
			SolrUtils.pinCertificate(cmd.getOptionValue("solr-pin-certificate"));
		}

		final SolrClient client = new HttpSolrClient(cmd.getOptionValue('s'));

		try {
			client.rollback();
			client.close();
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
