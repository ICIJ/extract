package org.icij.extract.cli.options;

import org.apache.commons.cli.Option;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public class SolrOptionSet extends OptionSet {

	public SolrOptionSet() {
		super(Option.builder("s")
				.desc("Solr core address. Required.")
				.longOpt("address")
				.hasArg()
				.argName("address")
				.required(true)
				.build(),

			Option.builder()
				.desc("The Solr server's public certificate, used for certificate pinning. Supported formats are PEM, DER, PKCS #12 and JKS.")
				.longOpt("pin-certificate")
				.hasArg()
				.argName("path")
				.build(),

			Option.builder()
				.desc("Verify the server's public certificate against the specified host. Use the wildcard \"*\" to disable verification.")
				.longOpt("verify-host")
				.hasArg()
				.argName("hostname")
				.build());
	}
}
