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
public class SolrCommitCli extends Cli {

	public SolrCommitCli(Logger logger) {
		super(logger, new String[] {
			"v", "s", "solr-pin-certificate", "soft"
		});
	}

	protected Option createOption(String name) {
		switch (name) {

		case "soft": return Option.builder()
			.desc("Performs a soft commit. Makes index changes visible while neither fsync-ing index files nor writing a new index descriptor. This could lead to data loss if Solr is terminated unexpectedly.")
			.longOpt(name)
			.build();

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
		final SolrClient client = new HttpSolrClient(cmd.getOptionValue('s'));

		try {
			if (cmd.hasOption("soft")) {
				client.commit(true, true, true);
			} else {
				client.commit(true, true, false);
			}
	
			client.close();
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
