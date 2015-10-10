package org.icij.extract.cli;

import org.icij.extract.core.*;
import org.icij.extract.cli.options.*;
import org.icij.extract.redis.Redis;

import java.util.Iterator;
import java.util.logging.Logger;

import java.io.IOException;

import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;

import org.redisson.Redisson;
import org.redisson.core.RMap;

import hu.ssh.progressbar.ProgressBar;
import hu.ssh.progressbar.console.ConsoleProgressBar;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public class CleanReportCli extends Cli {

	public CleanReportCli(Logger logger) {
		super(logger, new ReporterOptionSet(), new RedisOptionSet());
	}

	public CommandLine parse(String[] args) throws ParseException, IllegalArgumentException, RuntimeException {
		final CommandLine cmd = super.parse(args);

		final ReporterType reporterType = ReporterType.parse(cmd.getOptionValue('r', "redis"));

		if (ReporterType.REDIS != reporterType) {
			throw new IllegalArgumentException("Invalid reporter type: " + reporterType + ".");
		}

		final Redisson redisson = Redis.createClient(cmd.getOptionValue("redis-address"));
		final RMap<String, Integer> report = Redis.getReport(redisson, cmd.getOptionValue("report-name"));
		final Iterator<String> entries = report.keySet().iterator();

		final ProgressBar progressBar = ConsoleProgressBar.on(System.out)
			.withFormat("[:bar] :percent% :elapsed/:total ETA: :eta")
			.withTotalSteps(report.size());

		while (entries.hasNext()) {
			String path = entries.next();

			if (Files.notExists(Paths.get(path))) {
				entries.remove();
			}

			progressBar.tickOne();
		}

		redisson.shutdown();

		return cmd;
	}

	public void printHelp() {
		super.printHelp(Command.CLEAN_REPORT, "Remove any files that don't exist on disk from the report. The name option is respected.");
	}
}
