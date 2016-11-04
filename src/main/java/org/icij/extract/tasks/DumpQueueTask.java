package org.icij.extract.tasks;

import org.apache.commons.io.output.CloseShieldOutputStream;
import org.icij.extract.core.PathQueue;
import org.icij.extract.json.PathQueueSerializer;
import org.icij.extract.tasks.factories.PathQueueFactory;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.icij.task.MonitorableTask;
import org.icij.task.annotation.Option;
import org.icij.task.annotation.Task;

/**
 * A command that dumps a queue to JSON output.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
@Task("Dump the queue for debugging. The name option is respected. If no destination path is given then the " +
		"dump is written to standard output.")
@Option(name = "queue-type", description = "Set the queue backend type. For now, the only valid value is " +
		"\"redis\".", parameter = "type", code = "r")
@Option(name = "queue-name", description = "The name of the queue, the default of which is type-dependent" +
		".", parameter = "name")
@Option(name = "redis-address", description = "Set the Redis backend address. Defaults to " +
		"127.0.0.1:6379.", parameter = "address")
public class DumpQueueTask extends MonitorableTask<Integer> {

	@Override
	public Integer run(final String[] arguments) throws Exception {
		try (final OutputStream output = new FileOutputStream(arguments[0]);
		     final PathQueue queue = PathQueueFactory.createSharedQueue(options)) {
			monitor.hintRemaining(queue.size());
			dump(queue, output);
			return queue.size();
		} catch (FileNotFoundException e) {
			throw new RuntimeException(String.format("Unable to open \"%s\" for writing.", arguments[0]), e);
		}
	}

	@Override
	public Integer run() throws Exception {
		try (final OutputStream output = new CloseShieldOutputStream(System.out);
		final PathQueue queue = PathQueueFactory.createSharedQueue(options)) {
			monitor.hintRemaining(queue.size());
			dump(queue, output);
			return queue.size();
		}
	}

	/**
	 * Dump a queue as JSON to the given output stream.
	 *
	 * @param queue the queue to dump
	 * @param output the output stream to dump to
	 */
	private void dump(final PathQueue queue, final OutputStream output) throws IOException {
		final ObjectMapper mapper = new ObjectMapper();
		final SimpleModule module = new SimpleModule();

		module.addSerializer(PathQueue.class, new PathQueueSerializer(monitor));
		mapper.registerModule(module);

		try (final JsonGenerator jsonGenerator = new JsonFactory().setCodec(mapper).createGenerator(output,
				JsonEncoding.UTF8)) {
			jsonGenerator.useDefaultPrettyPrinter();
			jsonGenerator.writeObject(queue);
			jsonGenerator.writeRaw('\n');
		}
	}
}
