package org.icij.extract.cli;

import org.icij.extract.core.Queue;
import org.icij.extract.core.Scanner;

import org.icij.extract.cli.options.QueueOptionSet;
import org.icij.extract.cli.options.RedisOptionSet;
import org.icij.extract.cli.options.ScannerOptionSet;
import org.icij.extract.cli.factory.QueueFactory;

import java.util.logging.Logger;

import java.io.IOException;

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

	public CommandLine parse(String[] args) throws ParseException, IllegalArgumentException {
		final CommandLine cmd = super.parse(args);
		final String[] paths = cmd.getArgs();

		if (paths.length == 0) {
			throw new IllegalArgumentException("You must pass the paths to scan on the command line.");
		}

		final Queue queue = QueueFactory.createQueue(cmd);
		final Scanner scanner = new Scanner(logger, queue);
		final String base = cmd.getOptionValue("path-base");

		ScannerOptionSet.configureScanner(cmd, scanner);
		for (String path : paths) {
			scanner.scan(base, path);
		}

		scanner.shutdown();
		try {

			// Block until the scanning of each directory has completed in serial.
			scanner.awaitTermination();
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
