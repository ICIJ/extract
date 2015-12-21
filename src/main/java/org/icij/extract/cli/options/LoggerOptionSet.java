package org.icij.extract.cli.options;

import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.CommandLine;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public class LoggerOptionSet extends OptionSet {

	public LoggerOptionSet() {
		super(Option.builder("v")
				.desc("Set the log level. Either \"severe\", \"warning\" or \"info\". Defaults to \"warning\".")
				.longOpt("verbosity")
				.hasArg()
				.argName("level")
				.build());
	}

	public static void configureLogger(final CommandLine cmd, final Logger logger) {
		if (cmd.hasOption('v')) {
			logger.setLevel(Level.parse((cmd.getOptionValue('v')).toUpperCase(Locale.ROOT)));
		} else {
			logger.setLevel(Level.WARNING);
		}
	}
}
