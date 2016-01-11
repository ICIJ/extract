package org.icij.extract.cli;

import org.icij.extract.core.Report;
import org.icij.extract.core.ReportResult;
import org.icij.extract.cli.options.ReporterOptionSet;
import org.icij.extract.cli.options.RedisOptionSet;
import org.icij.extract.cli.factory.ReportFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;

import hu.ssh.progressbar.ProgressBar;
import hu.ssh.progressbar.console.ConsoleProgressBar;
import org.icij.extract.core.ReportType;

/**
 * CLI class for replacing the the paths in a report.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public class ReplaceReportCli extends Cli {

	public ReplaceReportCli(final Logger logger) {
		super(logger, new ReporterOptionSet(), new RedisOptionSet());
	}

	public CommandLine parse(final String[] args) throws ParseException, IllegalArgumentException {
		final CommandLine cmd = super.parse(args);

		final Report report = ReportFactory.createReport(cmd, ReportType.REDIS);
		ReportResult match = null;

		final String[] literals = cmd.getArgs();
		final String search;
		final String replacement;

		if (literals.length < 1) {
			throw new IllegalArgumentException("You must pass the search string on the command line.");
		}

		search = literals[0];
		if (literals.length < 2) {
			replacement = "";
		} else {
			replacement = literals[1];
		}

		final Pattern pattern = Pattern.compile(search);
		final ProgressBar progressBar = ConsoleProgressBar.on(System.err)
				.withFormat("[:bar] :percent% :elapsed/:total ETA: :eta")
				.withTotalSteps(report.size());
		final Iterator<Map.Entry<Path, ReportResult>> iterator = report.entrySet().iterator();

		while (iterator.hasNext()) {
			Map.Entry<Path, ReportResult> entry = iterator.next();
			Matcher matcher = pattern.matcher(entry.getKey().toString());

			if (matcher.matches()) {
				report.put(Paths.get(matcher.replaceFirst(replacement)), entry.getValue());
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
		super.printHelp(Command.REPLACE_REPORT, "Replace the given search string in paths with the replacement string.",
				"search", "replacement");
	}
}
