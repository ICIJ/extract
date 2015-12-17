package org.icij.extract.cli;

import org.icij.extract.core.Report;
import org.icij.extract.cli.options.ReporterOptionSet;
import org.icij.extract.cli.options.RedisOptionSet;
import org.icij.extract.cli.factory.ReportFactory;

import java.io.IOException;

import java.util.logging.Logger;

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
public class WipeReportCli extends Cli {

	public WipeReportCli(Logger logger) {
		super(logger, new ReporterOptionSet(), new RedisOptionSet());
	}

	public CommandLine parse(final String[] args) throws ParseException, IllegalArgumentException {
		final CommandLine cmd = super.parse(args);

		final Report report = ReportFactory.createReport(cmd);

		logger.info("Wiping report.");
		report.clear();

		try {
			report.close();
		} catch (IOException e) {
			throw new RuntimeException("Exception while closing queue.", e);
		}

		return cmd;
	}

	public void printHelp() {
		super.printHelp(Command.WIPE_REPORT, "Wipe a report. The name option is respected.");
	}
}
