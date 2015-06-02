package org.icij.extract.cli;

import org.icij.extract.core.*;

import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import java.io.IOException;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;

import org.redisson.Redisson;
import org.redisson.core.RQueue;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;

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
			"v", "q", "n", "redis-address"
		});
	}

	protected Option createOption(String name) {
		switch (name) {

		case "n": return Option.builder("n")
			.desc("The name of the queue to dump from. Defaults to \"extract\".")
			.longOpt("name")
			.hasArg()
			.argName("name")
			.build();

		case "q": return Option.builder("q")
			.desc("Set the queue backend type. For now, the only valid value and the default is \"redis\".")
			.longOpt("queue")
			.hasArg()
			.argName("type")
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

		final QueueType queueType = QueueType.parse(cmd.getOptionValue('q', "redis"));

		if (QueueType.REDIS != queueType) {
			throw new IllegalArgumentException("Invalid queue type: " + queueType + ".");
		}

		final Redisson redisson = getRedisson(cmd);
		final RQueue<String> queue = redisson.getQueue(cmd.getOptionValue('n', "extract") + ":queue");

		final Iterator<String> files = queue.iterator();

		try {
			final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(System.out));
			final JsonGenerator jsonGenerator = new JsonFactory().createJsonGenerator(writer);

			jsonGenerator.useDefaultPrettyPrinter();
			jsonGenerator.writeStartArray();

			while (files.hasNext()) {
				jsonGenerator.writeString((String) files.next());
			}

			jsonGenerator.writeEndArray();
			jsonGenerator.close();

			writer.newLine();
			writer.close();
		} catch (IOException e) {
			throw new RuntimeException("Unable to output JSON.", e);
		}

		redisson.shutdown();

		return cmd;
	}

	public void printHelp() {
		super.printHelp(Command.DUMP_QUEUE, "Dump the queue for debugging. The name option is respected.");
	}
}
