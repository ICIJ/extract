package org.icij.extract.tasks;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.commons.io.output.CloseShieldOutputStream;
import org.icij.extract.json.DocumentQueueSerializer;
import org.icij.extract.queue.DocumentQueue;
import org.icij.extract.queue.DocumentQueueFactory;
import org.icij.task.MonitorableTask;
import org.icij.task.annotation.OptionsClass;
import org.icij.task.annotation.Task;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;

/**
 * A command that dumps a queue to JSON output.
 *
 *
 */
@Task("Dump the queue for debugging. The name option is respected. If no destination path is given then the " +
		"dump is written to standard output.")
@OptionsClass(DocumentQueueFactory.class)
public class DumpQueueTask extends MonitorableTask<Void> {

	@Override
	public Void call(final String[] arguments) throws Exception {
		try (final OutputStream output = new BufferedOutputStream(new FileOutputStream(arguments[0]));
		     final DocumentQueue<Path> queue = new DocumentQueueFactory(options).createShared(Path.class)) {
			monitor.hintRemaining(queue.size());
			dump(queue, output);
		} catch (FileNotFoundException e) {
			throw new RuntimeException(String.format("Unable to open \"%s\" for writing.", arguments[0]), e);
		}

		return null;
	}

	@Override
	public Void call() throws Exception {
		try (final OutputStream output = new BufferedOutputStream(new CloseShieldOutputStream(System.out));
		final DocumentQueue<Path> queue = new DocumentQueueFactory(options).createShared(Path.class)) {
			monitor.hintRemaining(queue.size());
			dump(queue, output);
		}

		return null;
	}

	/**
	 * Dump a queue as JSON to the given output stream.
	 *
	 * @param queue the queue to dump
	 * @param output the output stream to dump to
	 */
	private void dump(final DocumentQueue<Path> queue, final OutputStream output) throws IOException {
		final ObjectMapper mapper = new ObjectMapper();
		final SimpleModule module = new SimpleModule();

		module.addSerializer(DocumentQueue.class, new DocumentQueueSerializer(monitor));
		mapper.registerModule(module);

		try (final JsonGenerator jsonGenerator = new JsonFactory().setCodec(mapper).createGenerator(output,
				JsonEncoding.UTF8)) {
			jsonGenerator.useDefaultPrettyPrinter();
			jsonGenerator.writeObject(queue);
			jsonGenerator.writeRaw('\n');
		}
	}
}
