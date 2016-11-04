package org.icij.extract.tasks;

import org.apache.commons.io.input.CloseShieldInputStream;
import org.icij.extract.core.PathQueue;
import org.icij.extract.json.PathQueueDeserializer;
import org.icij.extract.tasks.factories.PathQueueFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

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
@Task("Load a queue from a JSON dump file. The name option is respected. If no source path is given then the" +
		" input is read from standard input.")
@Option(name = "queue-type", description = "Set the report backend type. For now, the only valid value is " +
		"\"redis\".", parameter = "type", code = "r")
@Option(name = "queue-name", description = "The name of the report, the default of which is type-dependent" +
		".", parameter = "name")
@Option(name = "redis-address", description = "Set the Redis backend address. Defaults to " +
		"127.0.0.1:6379.", parameter = "address")
public class LoadQueueTask extends DefaultTask<Integer> {

	@Override
	public Integer run() throws Exception {
		try (final InputStream input = new CloseShieldInputStream(System.in);
		     final PathQueue queue = PathQueueFactory.createSharedQueue(options)) {
			return load(queue, input);
		}
	}

	@Override
	public Integer run(final String[] arguments) throws Exception {
		int i = 0;

		try (final PathQueue queue = PathQueueFactory.createSharedQueue(options)) {
			for (String argument : arguments) {
				i = load(queue, argument);
			}
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Unable to open dump file for reading.", e);
		}

		return i;
	}

	/**
	 * Load a dump file from the given path into a queue.
	 *
	 * @param queue the queue to load the dump into
	 * @param path the path to load the dump from
	 * @return the new size of the queue
	 * @throws IOException if the dump could not be loaded
	 */
	private Integer load(final PathQueue queue, final String path) throws IOException {
		try (final InputStream input = new FileInputStream(path)) {
			return load(queue, input);
		}
	}

	/**
	 * Load a dump file from the given input stream into a queue.
	 *
	 * @param queue the queue to load the dump into
	 * @param input the input stream to load the dump from
	 * @return the new size of the queue
	 * @throws IOException if the dump could not be loaded
	 */
	private Integer load(final PathQueue queue, final InputStream input) throws IOException {
		final ObjectMapper mapper = new ObjectMapper();
		final SimpleModule module = new SimpleModule();

		module.addDeserializer(PathQueue.class, new PathQueueDeserializer(queue));
		mapper.registerModule(module);

		try (final JsonParser jsonParser = new JsonFactory().setCodec(mapper).createParser(input)) {
			return jsonParser.readValueAs(PathQueue.class).size();
		}
	}
}
