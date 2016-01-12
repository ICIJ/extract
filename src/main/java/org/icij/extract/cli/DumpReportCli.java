package org.icij.extract.cli;

import org.icij.extract.core.Report;
import org.icij.extract.core.ExtractionResult;
import org.icij.extract.core.ReportType;
import org.icij.extract.json.ReportSerializer;
import org.icij.extract.cli.options.ReporterOptionSet;
import org.icij.extract.cli.options.RedisOptionSet;
import org.icij.extract.cli.factory.ReportFactory;

import java.util.logging.Logger;

import java.io.IOException;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

import hu.ssh.progressbar.ProgressBar;
import hu.ssh.progressbar.console.ConsoleProgressBar;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public class DumpReportCli extends Cli {

	public DumpReportCli(final Logger logger) {
		super(logger, new ReporterOptionSet(), new RedisOptionSet());

		options.addOption(Option.builder()
			.desc("Only dump reports matching the given status.")
			.longOpt("reporter-status")
			.hasArg()
			.argName("status")
			.build());
	}

	public CommandLine parse(final String[] args) throws ParseException, IllegalArgumentException {
		final CommandLine cmd = super.parse(args);

		final Report report = ReportFactory.createReport(cmd, ReportType.REDIS);
		ExtractionResult match = null;

		if (cmd.hasOption("reporter-status")) {
			match = ExtractionResult.get(((Number) cmd.getParsedOptionValue("reporter-status")));

			if (null == match) {
				throw new IllegalArgumentException(String.format("%s is not a valid report status.",
					cmd.getOptionValue("reporter-status")));
			}
		}

		final ProgressBar progressBar = ConsoleProgressBar.on(System.err)
			.withFormat("[:bar] :percent% :elapsed/:total ETA: :eta")
			.withTotalSteps(report.size());

		final ObjectMapper mapper = new ObjectMapper();
		final SimpleModule module = new SimpleModule();

		module.addSerializer(Report.class, new ReportSerializer(progressBar, match));
		mapper.registerModule(module);

		try (
			final JsonGenerator jsonGenerator = new JsonFactory()
				.setCodec(mapper)
				.createGenerator(System.out, JsonEncoding.UTF8)
		) {
			jsonGenerator.useDefaultPrettyPrinter();
			jsonGenerator.writeObject(report);
			jsonGenerator.writeRaw('\n');
		} catch (IOException e) {
			throw new RuntimeException("Unable to output JSON.", e);
		}

		try {
			report.close();
		} catch (IOException e) {
			throw new RuntimeException("Exception while closing report.", e);
		}

		return cmd;
	}

	public void printHelp() {
		super.printHelp(Command.DUMP_REPORT, "Dump the report for debugging. The name option is respected.");
	}
}
