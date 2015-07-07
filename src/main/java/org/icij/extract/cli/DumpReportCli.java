package org.icij.extract.cli;

import org.icij.extract.core.*;

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
		super(logger, new String[] {
			"v", "r", "n", "redis-address", "reporter-status"
		});
	}

	protected Option createOption(String name) {
		switch (name) {

		case "n": return Option.builder("n")
			.desc("The name of the report to dump from. Defaults to \"extract\".")
			.longOpt("name")
			.hasArg()
			.argName("name")
			.build();

		case "r": return Option.builder("r")
			.desc("Set the reporter backend type. For now, the only valid value and the default is \"redis\".")
			.longOpt("reporter")
			.hasArg()
			.argName("type")
			.build();

		case "reporter-status": return Option.builder()
			.desc("Only dump reports matching the given status.")
			.longOpt(name)
			.hasArg()
			.argName("status")
			.type(Number.class)
			.required(true)
			.build();

		case "redis-address": return Option.builder()
			.desc("Set the Redis backend address. Defaults to 127.0.0.1:6379.")
			.longOpt(name)
			.hasArg()
			.argName("address")
			.build();

		default:
			return super.createOption(name);
		}
	}

	public CommandLine parse(String[] args) throws ParseException, IllegalArgumentException, RuntimeException {
		final CommandLine cmd = super.parse(args);

		final ReporterType reporterType = ReporterType.parse(cmd.getOptionValue('r'));

		if (ReporterType.REDIS != reporterType) {
			throw new IllegalArgumentException("Invalid reporter type: " + reporterType + ".");
		}

		final Redisson redisson = getRedisson(cmd);
		final RMap<String, Integer> report = redisson.getMap(cmd.getOptionValue('n', "extract") + ":report");

		final int status = ((Number) cmd.getParsedOptionValue("reporter-status")).intValue();

		final Iterator<Map.Entry<String, Integer>> entries = report.entrySet().iterator();

		try {
			final JsonGenerator jsonGenerator = new JsonFactory()
				.createGenerator(System.out, JsonEncoding.UTF8);

			jsonGenerator.useDefaultPrettyPrinter();
			jsonGenerator.writeStartArray();

			while (entries.hasNext()) {
				Map.Entry<String, Integer> entry = (Map.Entry<String, Integer>) entries.next();

				if (entry.getValue() == status) {
					jsonGenerator.writeString((String) entry.getKey());
				}
			}

			jsonGenerator.writeEndArray();
			jsonGenerator.writeRaw('\n');
			jsonGenerator.close();
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
