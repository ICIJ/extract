package org.icij.extract.cli.options;

import org.icij.extract.core.*;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.CommandLine;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public class ConsumerOptionSet extends OptionSet {

	public ConsumerOptionSet() {
		super(Option.builder()
				.desc(String.format("Time to wait when polling the queue e.g. \"5s\" or \"1m\". Defaults to \"%s\".",
					PollingConsumer.DEFAULT_TIMEOUT))
				.longOpt("queue-poll")
				.hasArg()
				.argName("duration")
				.build());
	}

	public static void configureConsumer(final CommandLine cmd, final PollingConsumer consumer) {
		if (cmd.hasOption("queue-poll")) {
			consumer.setPollTimeout(cmd.getOptionValue("queue-poll"));
		}
	}
}
