package org.icij.extract;

import java.lang.Runnable;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.ParseException;

import java.io.IOException;
import java.nio.file.Paths;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public class Cli {
	private static final Logger logger = Logger.getLogger(Cli.class.getName());
	private final String[] args;
	private final Options options = new Options();

	public Cli(String[] args) {
		this.args = args;

		this.options
			.addOption("h", "help", false, "Show help.")
			.addOption("d", "directory", true, "Directory to scan for documents. Defaults to the current directory.")
			.addOption("o", "output", true, "Directory to output extracted text. Defaults to outputting to standard output.")
			.addOption("e", "encoding", true, "Set Tika's output encoding. Defaults to UTF-8.")
			.addOption("i", "include", true, "Glob pattern for matching files e.g. \"*.{tif,pdf}\". Files not matching the pattern will be ignored.")
			.addOption("x", "exclude", true, "Glob pattern for excluding files and directories. Files and directories matching the pattern will be ignored.")
			.addOption("f", "follow", false, "Follow symbolic links when scanning for documents.")
			.addOption("l", "language", true, "Set the language used by Tesseract. Defaults to \"eng\".")
			.addOption("a", "detect-language", false, "Guess the language of text extracted via OCR using Tika's language detection, running the OCR a second time if a new language is detected.")
			.addOption("v", "verbosity", true, "Set the log level. Either \"SEVERE\", \"WARNING\" or \"INFO\".")
			.addOption(OptionBuilder.withLongOpt("threads")
				.hasArg()
				.withType(Number.class)
				.withDescription("Number of threads to use while processing. Affects the number of files which are processed concurrently. Defaults to the number of available processors.")
				.create("t"));
	}

	public Runnable parse() throws ParseException {
		final CommandLineParser parser = new DefaultParser();

		CommandLine cmd = null;
		Runnable job = null;

		try {
			cmd = parser.parse(options, args);
		} catch (ParseException e) {
			logger.log(Level.SEVERE, "Failed to parse command line arguments.", e);
			throw e;
		}

		if (cmd.hasOption('v')) {
			logger.setLevel(Level.parse((String) cmd.getOptionValue('v')));
		} else {
			logger.setLevel(Level.SEVERE);
		}

		if (cmd.hasOption('h')) {
			help();
			return job;
		}

		int threads = Queue.DEFAULT_THREADS;

		if (cmd.hasOption('t')) {
			try {
				threads = ((Number) cmd.getParsedOptionValue("t")).intValue();
			} catch (ParseException e) {
				logger.log(Level.SEVERE, "Invalid value for thread count.", e);
				throw e;
			}
		}

		Spewer spewer;

		if (cmd.hasOption('o')) {
			spewer = new FileSpewer(logger);
			((FileSpewer) spewer).setOutputDirectory(Paths.get((String) cmd.getOptionValue('o')));
		} else {
			spewer = new StdOutSpewer();
		}

		final Consumer consumer = new Consumer(logger, spewer);
		final Queue queue = new Queue(logger, consumer, threads);

		if (cmd.hasOption('e')) {
			consumer.setOutputEncoding((String) cmd.getOptionValue('e'));
		}

		if (cmd.hasOption('l')) {
			consumer.setOcrLanguage((String) cmd.getOptionValue('l'));
		}

		if (cmd.hasOption('a')) {
			consumer.detectLanguageForOcr();
		}

		final String directory = (String) cmd.getOptionValue('d', ".");
		final Scanner scanner = new Scanner(logger, queue);

		if (cmd.hasOption('i')) {
			scanner.setIncludeGlob((String) cmd.getOptionValue('i'));
		}

		if (cmd.hasOption('x')) {
			scanner.setExcludeGlob((String) cmd.getOptionValue('x'));
		}

		if (cmd.hasOption('f')) {
			scanner.followSymLinks();
		}

		job = new Runnable() {
			@Override
			public void run() {
				try {
					scanner.scan(Paths.get(directory));
				} catch (IOException e) {
					logger.log(Level.SEVERE, "Error while scanning directory.", e);
				}

				queue.end();
			}
		};

		return job;
	}

	public void help() {
		HelpFormatter formatter = new HelpFormatter();

		formatter.printHelp("extract", options);
	}
}
