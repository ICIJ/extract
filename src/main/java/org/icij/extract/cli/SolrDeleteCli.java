package org.icij.extract.cli;

import org.icij.extract.core.*;

import java.util.List;
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
		super(logger, new String[] {
			"v", "s", "solr-pin-certificate", "solr-verify-host", "i"
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

		final CloseableHttpClient httpClient = ClientUtils.createHttpClient(cmd.getOptionValue("solr-pin-certificate"), cmd.getOptionValue("solr-verify-host"));
		final SolrClient client = new HttpSolrClient(cmd.getOptionValue('s'), httpClient);

		final String idField = cmd.getOptionValue('i', "id");
		final List<String> ids = cmd.getArgList();

		if (ids.size() == 0) {
			throw new IllegalArgumentException("You must pass the IDs to delete on the command line.");
		}

		try {
			for (String id : ids) {
				if (id.contains("*")) {
					client.deleteByQuery(idField + ":" + id);
				} else {
					client.deleteById(id);
				}
			}

			client.close();
			httpClient.close();
		} catch (SolrServerException e) {
			throw new RuntimeException("Unable to delete.", e);
		} catch (IOException e) {
			throw new RuntimeException("There was an error while communicating with Solr.", e);
		}

		return cmd;
	}

	public void printHelp() {
		super.printHelp(Command.SOLR_DELETE, "Delete files from Solr.");
	}
}
