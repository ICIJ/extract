package org.icij.extract.cli;

import org.icij.extract.core.Report;
import org.icij.extract.cli.options.ReporterOptionSet;
import org.icij.extract.cli.options.RedisOptionSet;
import org.icij.extract.cli.factory.ReportFactory;

import java.util.Iterator;
import java.util.logging.Logger;

import java.io.IOException;

import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;

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

		final Report report = ReportFactory.createReport(cmd);
		final Iterator<Path> iterator = report.keySet().iterator();

		final ProgressBar progressBar = ConsoleProgressBar.on(System.out)
			.withFormat("[:bar] :percent% :elapsed/:total ETA: :eta")
			.withTotalSteps(report.size());

		while (iterator.hasNext()) {
			Path path = iterator.next();

			if (Files.notExists(path)) {
				iterator.remove();
			}

			progressBar.tickOne();
		}

		try {
			report.close();
		} catch (IOException e) {
			throw new RuntimeException("Exception while closing report.", e);
		}

		return cmd;
	}

	public void printHelp() {
		super.printHelp(Command.CLEAN_REPORT, "Remove any files that don't exist on disk from the report. The name option is respected.");
	}
}
