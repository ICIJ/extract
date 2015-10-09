package org.icij.extract.cli;

import org.icij.extract.core.*;
import org.icij.extract.cli.options.*;
import org.icij.extract.redis.Redis;

import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.logging.Logger;

import java.io.IOException;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;

import org.redisson.Redisson;
import org.redisson.core.RMap;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonEncoding;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public class DumpReportCli extends Cli {

	public DumpReportCli(Logger logger) {
		super(logger, new ReporterOptionSet(), new RedisOptionSet());

		options.addOption(Option.builder()
			.desc("Only dump reports matching the given status.")
			.longOpt("reporter-status")
			.hasArg()
			.argName("status")
			.type(Number.class)
			.build());
	}

	public CommandLine parse(String[] args) throws ParseException, IllegalArgumentException, RuntimeException {
		final CommandLine cmd = super.parse(args);

		final ReporterType reporterType = ReporterType.parse(cmd.getOptionValue('r', "redis"));

		if (ReporterType.REDIS != reporterType) {
			throw new IllegalArgumentException("Invalid reporter type: " + reporterType + ".");
		}

		final Redisson redisson = Redis.createClient(cmd.getOptionValue("redis-address"));
		final RMap<String, Integer> report = Redis.getReport(redisson, cmd.getOptionValue("report-name"));

		Number status = null;
		if (cmd.hasOption("reporter-status")) {
			status = (Number) cmd.getParsedOptionValue("reporter-status");
		}

		final Iterator<Map.Entry<String, Integer>> entries = report.entrySet().iterator();

		try (
			final JsonGenerator jsonGenerator = new JsonFactory()
				.createGenerator(System.out, JsonEncoding.UTF8);
		) {
			jsonGenerator.useDefaultPrettyPrinter();
			jsonGenerator.writeStartObject();

			while (entries.hasNext()) {
				Map.Entry<String, Integer> entry = (Map.Entry<String, Integer>) entries.next();

				if (null == status || entry.getValue() == status.intValue()) {
					jsonGenerator.writeObjectField((String) entry.getKey(), entry.getValue());
				}
			}

			jsonGenerator.writeEndObject();
			jsonGenerator.writeRaw('\n');
		} catch (IOException e) {
			throw new RuntimeException("Unable to output JSON.", e);
		}

		redisson.shutdown();

		return cmd;
	}

	public void printHelp() {
		super.printHelp(Command.DUMP_REPORT, "Dump the report for debugging. The name option is respected.");
	}
}
