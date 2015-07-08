package org.icij.extract.cli;

import org.icij.extract.core.*;

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

	public static void setScannerOptions(CommandLine cmd, Scanner scanner) {
		if (cmd.hasOption("include-pattern")) {
			scanner.setIncludeGlob((String) cmd.getOptionValue("include-pattern"));
		}

		if (cmd.hasOption("exclude-pattern")) {
			scanner.setExcludeGlob((String) cmd.getOptionValue("exclude-pattern"));
		}

		if (cmd.hasOption("follow-symlinks")) {
			scanner.followSymLinks(true);
		}
	}

	public QueueCli(Logger logger) {
		super(logger, new String[] {
			"v", "q", "n", "redis-address", "include-pattern", "exclude-pattern", "follow-symlinks"
		});
	}

	protected Option createOption(String name) {
		switch (name) {

		case "q": return Option.builder("q")
			.desc("Set the queue backend type. For now, the only valid values are \"redis\" and \"none\". Defaults to using Redis.")
			.longOpt("queue")
			.hasArg()
			.argName("type")
			.build();

		case "n": return Option.builder("n")
			.desc("Set the name for the queue. This is useful for avoiding conflicts with other jobs. Defaults to \"extract\".")
			.longOpt("name")
			.hasArg()
			.argName("name")
			.build();

		case "redis-address": return Option.builder()
			.desc("Set the Redis backend address. Defaults to 127.0.0.1:6379.")
			.longOpt(name)
			.hasArg()
			.argName("address")
			.build();

		case "include-pattern": return Option.builder()
			.desc("Glob pattern for matching files e.g. \"*.{tif,pdf}\". Files not matching the pattern will be ignored.")
			.longOpt(name)
			.hasArg()
			.argName("pattern")
			.build();

		case "exclude-pattern": return Option.builder()
			.desc("Glob pattern for excluding files and directories. Files and directories matching the pattern will be ignored.")
			.longOpt(name)
			.hasArg()
			.argName("pattern")
			.build();

		case "follow-symlinks": return Option.builder()
			.desc("Follow symbolic links when scanning for documents. Links are not followed by default.")
			.longOpt(name)
			.build();

		default:
			return super.createOption(name);
		}
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
		final RBlockingQueue<String> queue = redisson.getBlockingQueue(cmd.getOptionValue('n', "extract") + ":queue");
		final Scanner scanner = new QueueingScanner(logger, queue);

		setScannerOptions(cmd, scanner);
		for (String directory : directories) {
			scanner.scan(Paths.get(directory));
		}

		try {

			// Block until the scanning of each directory has completed in serial.
			scanner.awaitTermination();
		} catch (CancellationException | InterruptedException e) {
			throw new RuntimeException("Directory scanning was cancelled or interruped.", e);
		} catch (ExecutionException e) {
			throw new RuntimeException("An error occurred while scanning.", e);
		}

		redisson.shutdown();

		return cmd;
	}

	public void printHelp() {
		super.printHelp(Command.QUEUE, "Queue files for processing later.");
	}
}
