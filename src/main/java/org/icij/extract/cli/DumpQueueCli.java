package org.icij.extract.cli;

import org.icij.extract.core.Queue;
import org.icij.extract.json.QueueSerializer;
import org.icij.extract.cli.options.QueueOptionSet;
import org.icij.extract.cli.options.RedisOptionSet;
import org.icij.extract.cli.factory.QueueFactory;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.logging.Logger;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

import hu.ssh.progressbar.ProgressBar;
import hu.ssh.progressbar.console.ConsoleProgressBar;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
public class DumpQueueCli extends Cli {

	public DumpQueueCli(Logger logger) {
		super(logger, new QueueOptionSet(), new RedisOptionSet());
	}

	public CommandLine parse(final String[] args) throws ParseException, IllegalArgumentException {
		final CommandLine cmd = super.parse(args);

		final String[] files = cmd.getArgs();

		if (files.length > 1) {
			throw new IllegalArgumentException("Only one dump file path may be passed at a time.");
		}

		final Queue queue = QueueFactory.createSharedQueue(cmd);
		final OutputStream output;

		// Write to stdout if no path is specified.
		if (0 == files.length) {
			logger.info("No path given. Writing to standard output.");
			output = System.out;
		} else {
			try {
				output = new FileOutputStream(files[0]);
			} catch (FileNotFoundException e) {
				throw new RuntimeException("Unable to open dump file for writing.", e);
			}
		}

		final ProgressBar progressBar = ConsoleProgressBar.on(System.out)
			.withFormat("[:bar] :percent% :elapsed/:total ETA: :eta")
			.withTotalSteps(queue.size());

		final ObjectMapper mapper = new ObjectMapper();
		final SimpleModule module = new SimpleModule();

		module.addSerializer(Queue.class, new QueueSerializer(progressBar));
		mapper.registerModule(module);

		try (
			final JsonGenerator jsonGenerator = new JsonFactory()
				.setCodec(mapper)
				.createGenerator(output, JsonEncoding.UTF8)
		) {
			jsonGenerator.useDefaultPrettyPrinter();
			jsonGenerator.writeObject(queue);
			jsonGenerator.writeRaw('\n');
		} catch (IOException e) {
			throw new RuntimeException("Unable to output JSON.", e);
		}

		try {
			queue.close();
		} catch (IOException e) {
			throw new RuntimeException("Exception while closing queue.", e);
		}

		return cmd;
	}

	public void printHelp() {
		super.printHelp(Command.DUMP_QUEUE, "Dump the queue for debugging. The name option is respected. If no " +
						"destination path is given then the dump is written to standard output.",
				"[destination]");
	}
}
