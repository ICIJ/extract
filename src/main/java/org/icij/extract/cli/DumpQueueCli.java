package org.icij.extract.cli;

import org.icij.extract.core.*;
import org.icij.extract.cli.options.*;
import org.icij.extract.redis.Redis;

import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import java.io.IOException;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;

import org.redisson.Redisson;
import org.redisson.core.RQueue;

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
public class DumpQueueCli extends Cli {

	public DumpQueueCli(Logger logger) {
		super(logger, new QueueOptionSet(), new RedisOptionSet());
	}

	public CommandLine parse(String[] args) throws ParseException, IllegalArgumentException, RuntimeException {
		final CommandLine cmd = super.parse(args);

		final QueueType queueType = QueueType.parse(cmd.getOptionValue('q', "redis"));

		// For now, the only allowed value (and the default) is "redis".
		// Enforce this.
		if (QueueType.REDIS != queueType) {
			throw new IllegalArgumentException("Invalid queue type: " + queueType + ".");
		}

		final Redisson redisson = Redis.createClient(cmd.getOptionValue("redis-address"));
		final RQueue<String> queue = Redis.getQueue(redisson, cmd.getOptionValue("queue-name"));

		final Iterator<String> files = queue.iterator();

		try (
			final JsonGenerator jsonGenerator = new JsonFactory()
				.createGenerator(System.out, JsonEncoding.UTF8);
		) {
			jsonGenerator.useDefaultPrettyPrinter();
			jsonGenerator.writeStartArray();

			while (files.hasNext()) {
				jsonGenerator.writeString(files.next());
			}

			jsonGenerator.writeEndArray();
			jsonGenerator.writeRaw('\n');
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
