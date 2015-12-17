package org.icij.extract.cli;

import org.icij.extract.core.Report;
import org.icij.extract.json.ReportDeserializer;
import org.icij.extract.cli.options.ReporterOptionSet;
import org.icij.extract.cli.options.RedisOptionSet;
import org.icij.extract.cli.factory.ReportFactory;

import java.util.logging.Logger;

import java.io.File;
import java.io.IOException;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public class LoadReportCli extends Cli {

	public LoadReportCli(Logger logger) {
		super(logger, new ReporterOptionSet(), new RedisOptionSet());
	}

	public CommandLine parse(String[] args) throws ParseException, IllegalArgumentException {
		final CommandLine cmd = super.parse(args);

		final String[] files = cmd.getArgs();

		if (files.length == 0) {
			throw new IllegalArgumentException("Dump file path must be passed on the command line.");
		}

		if (files.length > 1) {
			throw new IllegalArgumentException("Only one dump file path may be passed at a time.");
		}

		final File file = new File(files[0]);
		final Report report = ReportFactory.createReport(cmd);

		final ObjectMapper mapper = new ObjectMapper();
		final SimpleModule module = new SimpleModule();

		module.addDeserializer(Report.class, new ReportDeserializer(report));
		mapper.registerModule(module);

		try (
			final JsonParser jsonParser = new JsonFactory()
				.setCodec(mapper)
				.createParser(file);
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
			"Load a report from a JSON dump file. The name option is respected.",
			"source");
	}
}
