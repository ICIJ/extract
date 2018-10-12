package org.icij.extract.cli;

import me.tongfei.progressbar.ProgressBar;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.UnrecognizedOptionException;
import org.icij.event.Monitorable;
import org.icij.event.listeners.ConsoleProgressListener;
import org.icij.extract.cli.tasks.HelpTask;
import org.icij.extract.cli.tasks.VersionTask;
import org.icij.extract.tasks.*;
import org.icij.task.*;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Properties;

/**
 * The main class for running the commandline application.
 */
public class Main {

	public static final DefaultTaskFactory taskFactory = new DefaultTaskFactory();

	/**
	 * Attempts to parse the given commandline arguments and executes the appropriate runner, exiting with an
	 * appropriate status.
	 *
	 * @param args the commandline arguments
	 */
	public static void main(final String[] args) {
		// System.setProperty("java.util.logging.config.file", "logging.properties");

		taskFactory.addTask("clean-report", CleanReportTask.class);
		taskFactory.addTask("commit", CommitTask.class);
		taskFactory.addTask("copy", CopyTask.class);
		taskFactory.addTask("delete", DeleteTask.class);
		taskFactory.addTask("dump-queue", DumpQueueTask.class);
		taskFactory.addTask("dump-report", DumpReportTask.class);
		taskFactory.addTask("load-queue", LoadQueueTask.class);
		taskFactory.addTask("load-report", LoadReportTask.class);
		taskFactory.addTask("view-report", ViewReportTask.class);
		taskFactory.addTask("queue", QueueTask.class);
		taskFactory.addTask("rehash", RehashTask.class);
		taskFactory.addTask("rollback", RollbackTask.class);
		taskFactory.addTask("spew", SpewTask.class);
		taskFactory.addTask("tag", TagTask.class);
		taskFactory.addTask("wipe-queue", WipeQueueTask.class);
		taskFactory.addTask("wipe-report", WipeReportTask.class);
		taskFactory.addTask("inspect-dump", InspectDumpTask.class);
		taskFactory.addTask("spew-dump", SpewDumpTask.class);
		taskFactory.addTask("help", HelpTask.class);
		taskFactory.addTask("version", VersionTask.class);

		try {
			parse(args);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(2);
			return;
		}

		System.exit(0);
	}

	private static void parse(final String[] args) throws Exception {
		if (null == args || 0 == args.length) {
			new HelpTask().call();
			return;
		}

		final String command = args[0];
		final DefaultTask<Object> task;

		try {
			task = taskFactory.getTask(command);
		} catch (IllegalArgumentException e) {
			System.err.println(String.format("Sorry, \"%s\" is not an Extract command.", command));
			System.exit(1);
			return;
		}

		final Options<String> options = task.options();
		final Option<String> propertiesOpt = options.add("options", StringOptionParser::new);

		final org.apache.commons.cli.Options cliOptions = new CommonsTransformer()
				.apply(options);
		final CommandLineParser parser = new DefaultParser();
		final CommandLine line;
		final ProgressBar progressBar;

		try {
			line = parser.parse(cliOptions, Arrays.copyOfRange(args, 1, args.length));
		} catch (UnrecognizedOptionException e) {
			System.err.println(e.getMessage());
			System.exit(1);
			return;
		}

		// Optionally load parameters from a properties file.
		options.get(propertiesOpt).parse().asPath().ifPresent((path -> {
			final Properties properties = new Properties();

			try {
				properties.load(Files.newBufferedReader(path));
			} catch (IOException e) {
				System.err.println(e.getMessage());
				System.exit(1);
				return;
			}

			for (String key: properties.stringPropertyNames()) {
				final Option<String> o = options.get(key);

				if (null == o) {
					System.err.println(String.format("The option \"%s\" does not exist.", key));
					System.exit(1);
					return;
				}

				// CLI parameters take precedence.
				if (!o.value().isPresent()) {
					o.update(properties.getProperty(key));
				}
			}
		}));

		if (task instanceof Monitorable) {
			progressBar = new ProgressBar(command, 0);
			((Monitorable) task).addListener(new ConsoleProgressListener(progressBar));
			progressBar.start();
		} else {
			progressBar = null;
		}

		if (line.getArgs().length > 0) {
			task.call(line.getArgs());
		} else {
			task.call();
		}

		if (null != progressBar) {
			progressBar.stop();
		}
	}
}
