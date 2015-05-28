package org.icij.extract.cli;

import org.icij.extract.core.*;

import java.util.logging.Logger;

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
			"v", "redis-namespace", "redis-address"
		});
	}

	public CommandLine parse(String[] args) throws ParseException, IllegalArgumentException {
		final CommandLine cmd = super.parse(args);

		final Redisson redisson = getRedisson(cmd);
		final RMap<String, Integer> report = RedisReporter.getReport(cmd.getOptionValue("redis-namespace"), redisson);

		logger.info("Wiping report.");
		report.clear();
		redisson.shutdown();

		return cmd;
	}

	public void printHelp() {
		super.printHelp(Command.WIPE_REPORT, "Wipe a report. The namespace option is respected.");
	}
}
