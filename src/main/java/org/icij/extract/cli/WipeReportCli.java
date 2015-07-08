package org.icij.extract.cli;

import org.icij.extract.core.*;
import org.icij.extract.cli.options.*;

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
		super(logger, new ReporterOptionSet(), new RedisOptionSet());
	}

	public CommandLine parse(String[] args) throws ParseException, IllegalArgumentException {
		final CommandLine cmd = super.parse(args);

		final ReporterType reporterType = ReporterType.parse(cmd.getOptionValue('r', "redis"));

		if (ReporterType.REDIS != reporterType) {
			throw new IllegalArgumentException("Invalid reporter type: " + reporterType + ".");
		}

		final Redisson redisson = getRedisson(cmd);
		final RMap<String, Integer> report = redisson.getMap(cmd.getOptionValue("report-name", "extract") + ":report");

		logger.info("Wiping report.");
		report.clear();
		redisson.shutdown();

		return cmd;
	}

	public void printHelp() {
		super.printHelp(Command.WIPE_REPORT, "Wipe a report. The name option is respected.");
	}
}
