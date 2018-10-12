package org.icij.extract.tasks;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.input.CloseShieldInputStream;

import org.icij.extract.document.DocumentFactory;
import org.icij.extract.queue.DocumentQueue;
import org.icij.extract.json.DocumentQueueDeserializer;
import org.icij.extract.queue.DocumentQueueFactory;

import java.io.*;
import java.nio.file.Paths;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.icij.task.DefaultTask;
import org.icij.task.annotation.Option;
import org.icij.task.annotation.OptionsClass;
import org.icij.task.annotation.Task;

/**
 * A command that loads a {@link DocumentQueue} from JSON.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
@Task("Load a queue from a JSON or CSV dump file. If no source path is given then the input is read from standard " +
		"input.")
@OptionsClass(DocumentQueueFactory.class)
@Option(name = "format", description = "The dump file format. Defaults to JSON.", parameter = "csv|json")
@Option(name = "pathField", description = "The name of CSV field to parse the path from.", parameter = "name")
public class LoadQueueTask extends DefaultTask<Void> {

	@Override
	public Void call() throws Exception {
		final DocumentFactory factory = new DocumentFactory().configure(options);

		try (final InputStream input = new CloseShieldInputStream(System.in);
		     final DocumentQueue queue = new DocumentQueueFactory(options)
				     .withDocumentFactory(factory)
				     .createShared()) {
			load(factory, queue, input);
		}

		return null;
	}

	@Override
	public Void call(final String[] arguments) throws Exception {
		final DocumentFactory factory = new DocumentFactory().configure(options);

		try (final DocumentQueue queue = new DocumentQueueFactory(options)
				.withDocumentFactory(factory)
				.createShared()) {
			for (String argument : arguments) {
				load(factory, queue, argument);
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
	private void load(final DocumentFactory factory, final DocumentQueue queue, final String path) throws IOException {
		try (final InputStream input = new BufferedInputStream(new FileInputStream(path))) {
			load(factory, queue, input);
		}
	}

	/**
	 * Load a dump file from the given path into a queue.
	 *
	 * @param queue the queue to load the dump into
	 * @param input the input stream to load the dump from
	 * @throws IOException if the dump could not be loaded
	 */
	private void load(final DocumentFactory factory, final DocumentQueue queue, final InputStream input) throws
			IOException {
		final String format = options.get("format").value().orElse("json");

		if (format.toLowerCase().equals("csv")) {
			loadFromCSV(factory, queue, input);
		} else {
			loadFromJSON(factory, queue, input);
		}
	}

	/**
	 * Load a dump file from the given input stream into a queue.
	 *
	 * @param queue the queue to load the dump into
	 * @param input the input stream to load the dump from
	 * @throws IOException if the dump could not be loaded
	 */
	private void loadFromCSV(final DocumentFactory factory, final DocumentQueue queue, final InputStream input) throws
			IOException {
		final String pathField = options.get("pathField").value().orElse("path");

		for (CSVRecord record : CSVFormat.RFC4180.withHeader().parse(new InputStreamReader(input))) {
			queue.add(factory.create(Paths.get(record.get(pathField))));
		}
	}

	/**
	 * Load a JSON dump file from the given input stream into a queue.
	 *
	 * @param queue the queue to load the dump into
	 * @param input the input stream to load the dump from
	 * @throws IOException if the dump could not be loaded
	 */
	private void loadFromJSON(final DocumentFactory factory, final DocumentQueue queue, final InputStream input)
			throws IOException {
		final ObjectMapper mapper = new ObjectMapper();
		final SimpleModule module = new SimpleModule();

		module.addDeserializer(DocumentQueue.class, new DocumentQueueDeserializer(factory, queue));
		mapper.registerModule(module);

		try (final JsonParser jsonParser = new JsonFactory().setCodec(mapper).createParser(input)) {
			jsonParser.readValueAs(DocumentQueue.class);
		}
	}
}
