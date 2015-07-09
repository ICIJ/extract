package org.icij.extract.cli.options;

import org.icij.extract.core.*;

import java.security.NoSuchAlgorithmException;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public class SolrSpewerOptionSet extends OptionSet {

	public SolrSpewerOptionSet() {
		super(Option.builder("s")
				.desc("Solr server address. Required if outputting to Solr.")
				.longOpt("solr-address")
				.hasArg()
				.argName("address")
				.build(),

			Option.builder()
				.desc("The Solr server's public certificate, used for certificate pinning. Supported formats are PEM, DER, PKCS #12 and JKS.")
				.longOpt("solr-pin-certificate")
				.hasArg()
				.argName("path")
				.build(),

			Option.builder()
				.desc("Verify the server's public certificate against the specified host. Use the wildcard \"*\" to disable verification.")
				.longOpt("solr-verify-host")
				.hasArg()
				.argName("hostname")
				.build(),

			Option.builder("i")
				.desc("Solr field for an automatically generated identifier. The ID for the same file is guaranteed not to change if the path doesn't change. Defaults to \"" + SolrSpewer.DEFAULT_ID_FIELD + "\".")
				.longOpt("solr-id-field")
				.hasArg()
				.argName("name")
				.build(),

			Option.builder()

				// The standard names are defined in the Oracle Standard Algorithm Name Documentation:
				// http://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html#MessageDigest
				.desc("The hashing algorithm used for generating Solr document identifiers e.g. \"MD5\" or \"SHA-256\". Turned off by default, so no ID is added.")
				.longOpt("solr-id-algorithm")
				.hasArg()
				.argName("name")
				.build(),

			Option.builder("t")
				.desc("Solr field for extracted text. Defaults to \"" + SolrSpewer.DEFAULT_TEXT_FIELD + "\".")
				.longOpt("solr-text-field")
				.hasArg()
				.argName("name")
				.build(),

			Option.builder("f")
				.desc("Solr field for the file path. Defaults to \"" + SolrSpewer.DEFAULT_PATH_FIELD + "\".")
				.longOpt("solr-path-field")
				.hasArg()
				.argName("name")
				.build(),

			Option.builder()
				.desc("Prefix for metadata fields added to Solr. Defaults to \"" + SolrSpewer.DEFAULT_METADATA_FIELD_PREFIX + "\".")
				.longOpt("solr-metadata-prefix")
				.hasArg()
				.argName("name")
				.build(),

			Option.builder()
				.desc("Commit to Solr every time the specified number of documents is added. Disabled by default. Consider using the \"autoCommit\" \"maxDocs\" directive in your Solr update handler configuration instead.")
				.longOpt("solr-commit-interval")
				.hasArg()
				.argName("interval")
				.type(Number.class)
				.build(),

			Option.builder()
				.desc("Instruct Solr to automatically commit a document after the specified amount of time has elapsed since it was added. Disabled by default. Consider using the \"autoCommit\" \"maxTime\" directive in your Solr update handler configuration instead.")
				.longOpt("solr-commit-within")
				.hasArg()
				.argName("duration")
				.build(),

			Option.builder()
				.desc("Make atomic updates to Solr. If your schema contains fields that are not included in the payload, this prevents their values, if any, from being erased.")
				.longOpt("solr-atomic-writes")
				.build(),

			Option.builder()
				.desc("Make all dates UTC. Tika's image metadata extractor will generate non-ISO compliant dates if the the timezone is not available in the source metadata. Turning this option on appends \"Z\" to non-compliant dates, making them compatible with the Solr date field type.")
				.longOpt("solr-utc-dates")
				.build());
	}

	public static void configureSpewer(final CommandLine cmd, final SolrSpewer spewer) throws ParseException {
		spewer.atomicWrites(cmd.hasOption("solr-atomic-writes"));
		spewer.utcDates(cmd.hasOption("solr-utc-dates"));

		if (cmd.hasOption('t')) {
			spewer.setTextField(cmd.getOptionValue('t'));
		}

		if (cmd.hasOption('f')) {
			spewer.setPathField(cmd.getOptionValue('f'));
		}

		if (cmd.hasOption('i')) {
			spewer.setIdField(cmd.getOptionValue('i'));
		}

		if (cmd.hasOption("solr-id-algorithm")) {
			try {
				spewer.setIdAlgorithm(cmd.getOptionValue("solr-id-algorithm"));
			} catch (NoSuchAlgorithmException e) {
				throw new IllegalArgumentException(String.format("Hashing algorithm \"%s\" not available on this platform.",
					cmd.getOptionValue("solr-id-algorithm")));
			}
		}

		if (cmd.hasOption("solr-metadata-prefix")) {
			spewer.setMetadataFieldPrefix(cmd.getOptionValue("solr-metadata-prefix"));
		}

		if (cmd.hasOption("solr-commit-interval")) {
			spewer.setCommitInterval(((Number) cmd.getParsedOptionValue("solr-commit-interval")).intValue());
		}

		if (cmd.hasOption("solr-commit-within")) {
			spewer.setCommitWithin(cmd.getOptionValue("solr-commit-within"));
		}
	}
}
