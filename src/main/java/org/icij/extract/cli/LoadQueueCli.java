package org.icij.extract.cli;

import org.icij.extract.core.*;

import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.logging.Logger;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileNotFoundException;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;

import org.redisson.Redisson;
import org.redisson.core.RQueue;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

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

		BufferedReader reader = null;

		try {
			reader = new BufferedReader(new FileReader(file));
		} catch (FileNotFoundException e) {
			throw new RuntimeException("The specified file does not exist: " + file + ".");
		}

		try {
			final JsonParser jsonParser = new JsonFactory().createParser(reader);

			while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
				queue.add(jsonParser.getValueAsString());
			}

			jsonParser.close();
			reader.close();
		} catch (IOException e) {
			throw new RuntimeException("Unable to load from JSON.", e);
		}

		redisson.shutdown();

		return cmd;
	}

	public void printHelp() {
		super.printHelp(Command.DUMP_QUEUE, "Dump the queue for debugging. The name option is respected.");
	}
}
