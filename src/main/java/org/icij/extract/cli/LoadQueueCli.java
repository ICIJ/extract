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
			"v", "n", "b", "redis-address"
		});
	}

	protected Option createOption(String name) {
		switch (name) {

		case "b": return Option.builder("b")
			.desc("Path to the backup JSON file. Required.")
			.longOpt("backup-file")
			.hasArg()
			.argName("path")
			.required(true)
			.build();

		default:
			return super.createOption(name);
		}
	}

	public CommandLine parse(String[] args) throws ParseException, IllegalArgumentException {
		final CommandLine cmd = super.parse(args);

		final Redisson redisson = getRedisson(cmd);
		final RQueue<String> queue = redisson.getQueue(cmd.getOptionValue('n', "extract") + ":queue");

		final String file = cmd.getOptionValue('f');
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
