package org.icij.extract.cli;

import org.icij.extract.core.*;

import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;

import org.redisson.Redisson;
import org.redisson.core.RQueue;

import javax.json.*;
import javax.json.stream.JsonGenerator;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public class DumpQueueCli extends Cli {

	public DumpQueueCli(Logger logger) {
		super(logger, new String[] {
			"v", "n", "redis-address"
		});
	}

	public CommandLine parse(String[] args) throws ParseException, IllegalArgumentException {
		final CommandLine cmd = super.parse(args);

		final Redisson redisson = getRedisson(cmd);
		final RQueue<String> queue = redisson.getQueue(cmd.getOptionValue('n', "extract") + ":queue");

		final Iterator<String> files = queue.iterator();
		final JsonArrayBuilder array = Json.createArrayBuilder();
		final Map<String, Boolean> config = new HashMap<>();

		config.put(JsonGenerator.PRETTY_PRINTING, Boolean.TRUE);

		final JsonWriterFactory factory = Json.createWriterFactory(config);
		final JsonWriter writer = factory.createWriter(System.out);

		while (files.hasNext()) {
			array.add((String) files.next());
		}

		writer.writeArray(array.build());
		System.out.print("\n");
		writer.close();

		redisson.shutdown();

		return cmd;
	}

	public void printHelp() {
		super.printHelp(Command.DUMP_QUEUE, "Dump the queue for debugging. The name option is respected.");
	}
}
