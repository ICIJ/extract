package org.icij.extract.tasks;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.input.CloseShieldInputStream;

import org.icij.extract.core.PathQueue;
import org.icij.extract.json.PathQueueDeserializer;
import org.icij.extract.tasks.factories.PathQueueFactory;

import java.io.*;
import java.nio.file.Paths;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.icij.task.DefaultTask;
import org.icij.task.annotation.Option;
import org.icij.task.annotation.Task;

/**
 * A command that loads a {@link PathQueue} from JSON.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
@Task("Load a queue from a JSON or CSV dump file. If no source path is given then the input is read from standard " +
		"input.")
@Option(name = "queue-type", description = "Set the report backend type. For now, the only valid value is " +
		"\"redis\".", parameter = "type", code = "r")
@Option(name = "queue-name", description = "The name of the queue, the default of which is type-dependent" +
		".", parameter = "name")
@Option(name = "format", description = "The dump file format. Defaults to JSON.", parameter = "csv|json")
@Option(name = "path-field", description = "The name of CSV field to get the path from.", parameter = "name")
@Option(name = "redis-address", description = "Set the Redis backend address. Defaults to " +
		"127.0.0.1:6379.", parameter = "address")
public class LoadQueueTask extends DefaultTask<Void> {

	@Override
	public Void run() throws Exception {
		try (final InputStream input = new CloseShieldInputStream(System.in);
		     final PathQueue queue = new PathQueueFactory(options).createShared()) {
			load(queue, input);
		}

		return null;
	}

	@Override
	public Void run(final String[] arguments) throws Exception {
		try (final PathQueue queue = new PathQueueFactory(options).createShared()) {
			for (String argument : arguments) {
				load(queue, argument);
			}
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Unable to open dump file for reading.", e);
		}

		return null;
	}

	/**
	 * Load a dump file from the given path into a queue.
	 *
	 * @param queue the queue to load the dump into
	 * @param path the path to load the dump from
	 * @throws IOException if the dump could not be loaded
	 */
	private void load(final PathQueue queue, final String path) throws IOException {
		try (final InputStream input = new BufferedInputStream(new FileInputStream(path))) {
			load(queue, input);
		}
	}

	/**
	 * Load a dump file from the given path into a queue.
	 *
	 * @param queue the queue to load the dump into
	 * @param input the input stream to load the dump from
	 * @throws IOException if the dump could not be loaded
	 */
	private void load(final PathQueue queue, final InputStream input) throws IOException {
		final String format = options.get("format").value().orElse("json");

		if (format.toLowerCase().equals("csv")) {
			loadFromCSV(queue, input);
		} else {
			loadFromJSON(queue, input);
		}
	}

	/**
	 * Load a dump file from the given input stream into a queue.
	 *
	 * @param queue the queue to load the dump into
	 * @param input the input stream to load the dump from
	 * @throws IOException if the dump could not be loaded
	 */
	private void loadFromCSV(final PathQueue queue, final InputStream input) throws IOException {
		final String pathField = options.get("path-field").value().orElse("path");

		for (CSVRecord record : CSVFormat.RFC4180.withHeader().parse(new InputStreamReader(input))) {
			queue.add(Paths.get(record.get(pathField)));
		}
	}

	/**
	 * Load a JSON dump file from the given input stream into a queue.
	 *
	 * @param queue the queue to load the dump into
	 * @param input the input stream to load the dump from
	 * @throws IOException if the dump could not be loaded
	 */
	private void loadFromJSON(final PathQueue queue, final InputStream input) throws IOException {
		final ObjectMapper mapper = new ObjectMapper();
		final SimpleModule module = new SimpleModule();

		module.addDeserializer(PathQueue.class, new PathQueueDeserializer(queue));
		mapper.registerModule(module);

		try (final JsonParser jsonParser = new JsonFactory().setCodec(mapper).createParser(input)) {
			jsonParser.readValueAs(PathQueue.class);
		}
	}
}
