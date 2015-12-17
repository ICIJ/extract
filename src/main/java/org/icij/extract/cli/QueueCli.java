package org.icij.extract.cli;

import org.icij.extract.core.Queue;
import org.icij.extract.core.Scanner;

import org.icij.extract.cli.options.QueueOptionSet;
import org.icij.extract.cli.options.RedisOptionSet;
import org.icij.extract.cli.options.ScannerOptionSet;
import org.icij.extract.cli.factory.QueueFactory;

import java.util.List;
import java.util.logging.Logger;

import java.io.IOException;

import java.nio.file.Path;
import java.nio.file.Paths;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

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
		final Queue queue = QueueFactory.createQueue(cmd);
		final List<String> directories = cmd.getArgList();

		if (directories.size() == 0) {
			throw new IllegalArgumentException("You must pass the directory paths to scan on the command line.");
		}

		final Scanner scanner = new Scanner(logger, queue);

		ScannerOptionSet.configureScanner(cmd, scanner);
		for (String directory : directories) {
			scanner.scan(Paths.get(directory));
		}

		try {

			// Block until the scanning of each directory has completed in serial.
			scanner.finish();
		} catch (InterruptedException e) {
			logger.warning("Interrupted.");
			Thread.currentThread().interrupt();
		}

		try {
			queue.close();
		} catch (IOException e) {
			throw new RuntimeException("Exception while closing queue.", e);
		}

		logger.info("Done.");

		return cmd;
	}

	public void printHelp() {
		super.printHelp(Command.QUEUE, "Queue files for processing later.",
			"paths...");
	}
}
