package org.icij.extract.cli;

import org.icij.extract.core.*;

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
			"v", "s", "pin-certificate", "verify-host", "i"
		});
	}

	protected Option createOption(String name) {
		switch (name) {

		case "s": return Option.builder("s")
			.desc("Solr server address. Required.")
			.longOpt("address")
			.hasArg()
			.argName("address")
			.required(true)
			.build();

		case "pin-certificate": return Option.builder()
			.desc("The Solr server's public certificate, used for certificate pinning. Supported formats are PEM, DER, PKCS #12 and JKS.")
			.longOpt(name)
			.hasArg()
			.argName("path")
			.build();

		case "verify-host": return Option.builder()
			.desc("Verify the server's public certificate against the specified host. Use the wildcard \"*\" to disable verification.")
			.longOpt(name)
			.hasArg()
			.argName("hostname")
			.build();

		case "i": return Option.builder("i")
			.desc("The name of the unique ID field in the target Solr schema.")
			.longOpt("id-field")
			.hasArg()
			.argName("name")
			.build();

		default:
			return super.createOption(name);
		}
	}

	public CommandLine parse(String[] args) throws ParseException, RuntimeException {
		final CommandLine cmd = super.parse(args);

		final CloseableHttpClient httpClient = ClientUtils
			.createHttpClient(cmd.getOptionValue("pin-certificate"), cmd.getOptionValue("verify-host"));
		final SolrClient client = new HttpSolrClient(cmd.getOptionValue('s'), httpClient);

		final String idField = cmd.getOptionValue('i', SolrSpewer.DEFAULT_ID_FIELD);
		final String[] ids = cmd.getArgs();

		if (0 == ids.length) {
			throw new IllegalArgumentException("You must pass the IDs to delete on the command line.");
		}

		try {
			for (String id : ids) {
				if (id.contains("*")) {
					logger.info(String.format("Deleting document matching ID pattern %s.", id));
					client.deleteByQuery(idField + ":" + id);
				} else {
					logger.info(String.format("Deleting document with ID %s.", id));
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
		super.printHelp(Command.SOLR_DELETE, "Delete documents from Solr.");
	}
}
