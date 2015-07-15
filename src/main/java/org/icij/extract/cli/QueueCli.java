package org.icij.extract.cli;

import org.icij.extract.core.*;
import org.icij.extract.cli.options.*;

import java.util.List;
import java.util.logging.Logger;

import java.nio.file.Path;
import java.nio.file.Paths;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

import org.redisson.Redisson;
import org.redisson.core.RBlockingQueue;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.CommandLine;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public class QueueCli extends Cli {

	public QueueCli(Logger logger) {
		super(logger, new QueueOptionSet(), new RedisOptionSet(), new ScannerOptionSet());
	}

	public CommandLine parse(String[] args) throws ParseException, IllegalArgumentException, RuntimeException {
		final CommandLine cmd = super.parse(args);

		final QueueType queueType = QueueType.parse(cmd.getOptionValue('q', "redis"));

		if (QueueType.REDIS != queueType) {
			throw new IllegalArgumentException("Invalid queue type: " + queueType + ".");
		}

		final List<String> directories = cmd.getArgList();

		if (directories.size() == 0) {
			throw new IllegalArgumentException("You must pass the directory paths to scan on the command line.");
		}

		final Redisson redisson = getRedisson(cmd);
		final RBlockingQueue<String> queue = redisson.getBlockingQueue(cmd.getOptionValue("queue-name", "extract") + ":queue");
		final Scanner scanner = new QueueingScanner(logger, queue, 1024);

		ScannerOptionSet.configureScanner(cmd, scanner);
		for (String directory : directories) {
			scanner.scan(Paths.get(directory));
		}

		try {

			// Block until the scanning of each directory has completed in serial.
			scanner.awaitTermination();
		} catch (InterruptedException e) {
			logger.warning("Interrupted.");
			Thread.currentThread().interrupt();
		}

		scanner.shutdown();

		logger.info("Shutting down Redis client.");
		redisson.shutdown();

		logger.info("Done.");

		return cmd;
	}

	public void printHelp() {
		super.printHelp(Command.QUEUE, "Queue files for processing later.",
			"paths...");
	}
}
