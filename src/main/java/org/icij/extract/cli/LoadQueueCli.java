package org.icij.extract.cli;

import org.icij.extract.core.*;

import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.logging.Logger;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

import org.apache.commons.cli.Option;
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
public class LoadQueueCli extends Cli {

	public LoadQueueCli(Logger logger) {
		super(logger, new String[] {
			"v", "n", "q", "redis-address"
		});
	}

	protected Option createOption(String name) {
		switch (name) {

		case "n": return Option.builder("n")
			.desc("The name of the queue to load into. Defaults to \"extract\".")
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

	public CommandLine parse(String[] args) throws ParseException, IllegalArgumentException {
		final CommandLine cmd = super.parse(args);

		final String[] files = cmd.getArgs();

		if (files.length == 0) {
			throw new IllegalArgumentException("Backup file path must be passed on the command line.");
		}

		if (files.length > 1) {
			throw new IllegalArgumentException("Only one backup file path may be passed at a time.");
		}

		final String file = files[0];
		final Redisson redisson = getRedisson(cmd);
		final RQueue<String> queue = redisson.getQueue(cmd.getOptionValue('n', "extract") + ":queue");
		JsonReader reader = null;

		try {
			reader = Json.createReader(new BufferedInputStream(new FileInputStream(file)));
		} catch (FileNotFoundException e) {
			throw new RuntimeException("No file exists at the given path: " + file + ".", e);
		}

		final JsonArray array = reader.readArray();

		try {
			for (int i = 0; i < array.size(); i++) {
				queue.add(array.getJsonString(i).getString());
			}
		} catch (IndexOutOfBoundsException | ClassCastException e) {
			throw new RuntimeException("Unexpected exception while load JSON.", e);
		}

		reader.close();
		redisson.shutdown();

		return cmd;
	}

	public void printHelp() {
		super.printHelp(Command.DUMP_QUEUE, "Dump the queue for debugging. The name option is respected.");
	}
}
