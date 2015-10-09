package org.icij.extract.cli;

import org.icij.extract.core.*;
import org.icij.extract.cli.options.*;
import org.icij.extract.redis.Redis;

import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.logging.Logger;

import java.io.File;
import java.io.IOException;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;

import org.redisson.Redisson;
import org.redisson.core.RQueue;

import com.fasterxml.jackson.core.JsonFactory;
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
		super(logger, new QueueOptionSet(), new RedisOptionSet());
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
		final Redisson redisson = Redis.createClient(cmd.getOptionValue("redis-address"));
		final RQueue<String> queue = Redis.getQueue(redisson, cmd.getOptionValue("queue-name"));

		try (
			final JsonParser jsonParser = new JsonFactory().createParser(file);
		) {
			jsonParser.nextToken(); // Skip over the start of the array.
			while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
				queue.add(jsonParser.getValueAsString());
			}
		} catch (IOException e) {
			throw new RuntimeException("Unable to load from JSON.", e);
		}

		redisson.shutdown();

		return cmd;
	}

	public void printHelp() {
		super.printHelp(Command.LOAD_QUEUE,
			"Load a queue from a JSON dump file. The name option is respected.",
			"source");
	}
}
