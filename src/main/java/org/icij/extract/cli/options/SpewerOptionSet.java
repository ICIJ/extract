package org.icij.extract.cli.options;

import org.icij.extract.core.*;

import java.util.Map;
import java.util.HashMap;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.CommandLine;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public class SpewerOptionSet extends OptionSet {

	public SpewerOptionSet() {
		super(Option.builder()
				.desc("Output metadata along with extracted text. For the \"file\" output type, a corresponding JSON file is created for every input file. With Solr, metadata fields are set using an optional prefix.")
				.longOpt("output-metadata")
				.build(),

			Option.builder()
				.desc("Set the given fields to their corresponding values on each document output.")
				.longOpt("tags")
				.hasArg()
				.argName("name and value pairs")
				.build(),

			Option.builder()
				.desc("Set the text output encoding. Defaults to UTF-8.")
				.longOpt("output-encoding")
				.hasArg()
				.argName("character set")
				.build());
	}

	public static void configureSpewer(final CommandLine cmd, final Spewer spewer) {
		if (cmd.hasOption("output-metadata")) {
			spewer.outputMetadata(true);
		}

		if (cmd.hasOption("tags")) {
			final String[] literals = cmd.getOptionValue("tags").split(" ");
			final Map<String, String> pairs = new HashMap<>();

			for (String literal : literals) {
				String[] pair = literal.split(":", 2);

				if (2 == pair.length) {
					pairs.put(pair[0], pair[1]);
				} else {
					throw new IllegalArgumentException(String.format("Invalid tag pair: \"%s\".", literal));
				}
			}

			spewer.setTags(pairs);
		}

		if (cmd.hasOption("output-encoding")) {
			spewer.setOutputEncoding(cmd.getOptionValue("output-encoding"));
		}
	}
}
