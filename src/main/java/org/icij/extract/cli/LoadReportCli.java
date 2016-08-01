package org.icij.extract.cli;

import org.icij.extract.core.Report;
import org.icij.extract.core.ReportType;
import org.icij.extract.json.ReportDeserializer;
import org.icij.extract.cli.options.ReporterOptionSet;
import org.icij.extract.cli.options.RedisOptionSet;
import org.icij.extract.cli.factory.ReportFactory;

import java.io.*;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

/**
 * CLI class for loading a report from JSON.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public class LoadReportCli extends Cli {

	public LoadReportCli(Logger logger) {
		super(logger, new ReporterOptionSet(), new RedisOptionSet());
	}

	public CommandLine parse(String[] args) throws ParseException, IllegalArgumentException {
		final CommandLine cmd = super.parse(args);

		final String[] files = cmd.getArgs();

		if (files.length > 1) {
			throw new IllegalArgumentException("Only one dump file path may be passed at a time.");
		}

		final InputStream input;

		// Write to stdout if no path is specified.
		if (0 == files.length) {
			logger.info("No path given. Reading from standard input.");
			input = System.in;
		} else {
			try {
				input = new FileInputStream(files[0]);
			} catch (FileNotFoundException e) {
				throw new RuntimeException("Unable to open dump file for reading.", e);
			}
		}

		final Report report = ReportFactory.createReport(cmd, ReportType.REDIS);

		final ObjectMapper mapper = new ObjectMapper();
		final SimpleModule module = new SimpleModule();

		module.addDeserializer(Report.class, new ReportDeserializer(report));
		mapper.registerModule(module);

		try (
			final JsonParser jsonParser = new JsonFactory()
				.setCodec(mapper)
				.createParser(input)
		) {
			jsonParser.readValueAs(Report.class);
		} catch (IOException e) {
			throw new RuntimeException("Unable to load from JSON.", e);
		}

		try {
			report.close();
		} catch (IOException e) {
			throw new RuntimeException("Exception while closing report.", e);
		}

		return cmd;
	}

	public void printHelp() {
		super.printHelp(Command.LOAD_REPORT,
			"Load a report from a JSON dump file. The name option is respected. If no source path is given then the " +
					"input is read from standard input.", "[source]");
	}
}
