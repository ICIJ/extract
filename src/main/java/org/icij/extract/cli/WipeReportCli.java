package org.icij.extract.cli;

import org.icij.extract.core.*;

import java.util.logging.Logger;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;

import org.redisson.Redisson;
import org.redisson.core.RMap;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public class WipeReportCli extends Cli {

	public WipeReportCli(Logger logger) {
		super(logger, new String[] {
			"v", "r", "n", "redis-address"
		});
	}

	protected Option createOption(String name) {
		switch (name) {

		case "n": return Option.builder("n")
			.desc("The name of the report to wipe. Defaults to \"extract\".")
			.longOpt("name")
			.hasArg()
			.argName("name")
			.build();

		case "r": return Option.builder("r")
			.desc("Set the reporter backend type. For now, the only valid value and the default is \"redis\".")
			.longOpt("reporter")
			.hasArg()
			.argName("type")
			.build();

		case "redis-address": return Option.builder()
			.desc("Set the Redis backend address. Defaults to 127.0.0.1:6379.")
			.longOpt(name)
			.hasArg()
			.argName("address")
			.build();

		default:
			return super.createOption(name);
		}
	}

	public CommandLine parse(String[] args) throws ParseException, IllegalArgumentException {
		final CommandLine cmd = super.parse(args);

		final ReporterType reporterType = ReporterType.parse(cmd.getOptionValue('r'));

		if (ReporterType.REDIS != reporterType) {
			throw new IllegalArgumentException("Invalid reporter type: " + reporterType + ".");
		}

		final Redisson redisson = getRedisson(cmd);
		final RMap<String, Integer> report = redisson.getMap(cmd.getOptionValue('n', "extract") + ":report");

		logger.info("Wiping report.");
		report.clear();
		redisson.shutdown();

		return cmd;
	}

	public void printHelp() {
		super.printHelp(Command.WIPE_REPORT, "Wipe a report. The name option is respected.");
	}
}
