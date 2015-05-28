package org.icij.extract.cli;

import org.icij.extract.core.*;

import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;

import org.redisson.Redisson;
import org.redisson.core.RMap;

import javax.json.*;
import javax.json.stream.JsonGenerator;

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
			"v", "redis-namespace", "redis-address", "reporter-status"
		});
	}

	public CommandLine parse(String[] args) throws ParseException, IllegalArgumentException {
		final CommandLine cmd = super.parse(args);

		final Redisson redisson = getRedisson(cmd);
		final RMap<String, Integer> report = RedisReporter.getReport(cmd.getOptionValue("redis-namespace"), redisson);

		final Iterator<Map.Entry<String, Integer>> entries = report.entrySet().iterator();
		final JsonArrayBuilder array = Json.createArrayBuilder();
		final Map<String, Boolean> config = new HashMap<>();

		config.put(JsonGenerator.PRETTY_PRINTING, Boolean.TRUE);

		final JsonWriterFactory factory = Json.createWriterFactory(config);
		final JsonWriter writer = factory.createWriter(System.out);

		final int status = ((Number) cmd.getParsedOptionValue("reporter-status")).intValue();

		while (entries.hasNext()) {
			Map.Entry<String, Integer> entry = (Map.Entry) entries.next();

			if (entry.getValue() == status) {
				array.add(entry.getKey());
			}
		}

		writer.writeArray(array.build());
		System.out.print("\n");
		writer.close();

		redisson.shutdown();

		return cmd;
	}

	public void printHelp() {
		super.printHelp(Command.DUMP_REPORT, "Dump the report for debugging. The namespace option is respected.");
	}
}
