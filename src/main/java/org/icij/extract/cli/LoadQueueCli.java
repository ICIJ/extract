package org.icij.extract.cli;

import org.icij.extract.core.Queue;
import org.icij.extract.json.QueueDeserializer;
import org.icij.extract.cli.options.QueueOptionSet;
import org.icij.extract.cli.options.RedisOptionSet;
import org.icij.extract.cli.factory.QueueFactory;

import java.util.logging.Logger;

import java.io.File;
import java.io.IOException;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

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

	public CommandLine parse(final String[] args) throws ParseException, IllegalArgumentException {
		final CommandLine cmd = super.parse(args);

		final String[] files = cmd.getArgs();

		if (files.length == 0) {
			throw new IllegalArgumentException("Dump file path must be passed on the command line.");
		}

		if (files.length > 1) {
			throw new IllegalArgumentException("Only one dump file path may be passed at a time.");
		}

		final File file = new File(files[0]);
		final Queue queue = QueueFactory.createQueue(cmd);

		final ObjectMapper mapper = new ObjectMapper();
		final SimpleModule module = new SimpleModule();

		module.addDeserializer(Queue.class, new QueueDeserializer(queue));
		mapper.registerModule(module);

		try (
			final JsonParser jsonParser = new JsonFactory()
				.setCodec(mapper)
				.createParser(file);
		) {
			jsonParser.readValueAs(Queue.class);
		} catch (IOException e) {
			throw new RuntimeException("Unable to load from JSON.", e);
		}

		try {
			queue.close();
		} catch (IOException e) {
			throw new RuntimeException("Exception while closing queue.", e);
		}

		return cmd;
	}

	public void printHelp() {
		super.printHelp(Command.LOAD_QUEUE,
			"Load a queue from a JSON dump file. The name option is respected.",
			"source");
	}
}
