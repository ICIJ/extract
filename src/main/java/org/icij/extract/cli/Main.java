package org.icij.extract.cli;

import me.tongfei.progressbar.ProgressBar;
import org.apache.commons.cli.*;

import org.icij.events.Monitorable;
import org.icij.events.listeners.ConsoleProgressListener;
import org.icij.extract.cli.tasks.HelpTask;
import org.icij.extract.cli.tasks.VersionTask;
import org.icij.extract.tasks.*;
import org.icij.task.DefaultTask;
import org.icij.task.DefaultTaskFactory;
import org.icij.task.MonitorableTask;
import org.icij.task.transformers.CommonsTransformer;

import java.util.Arrays;

/**
 * The main class for running the commandline application.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
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
		taskFactory.addTask("clean-report", CleanReportTask.class);
		taskFactory.addTask("commit", CommitTask.class);
		taskFactory.addTask("copy", CopyTask.class);
		taskFactory.addTask("delete", DeleteTask.class);
		taskFactory.addTask("dump-queue", DumpQueueTask.class);
		taskFactory.addTask("dump-report", DumpReportTask.class);
		taskFactory.addTask("load-queue", LoadQueueTask.class);
		taskFactory.addTask("load-report", LoadReportTask.class);
		taskFactory.addTask("queue", QueueTask.class);
		taskFactory.addTask("rehash", RehashTask.class);
		taskFactory.addTask("replace-report", ReplaceReportTask.class);
		taskFactory.addTask("rollback", RollbackTask.class);
		taskFactory.addTask("spew", SpewTask.class);
		taskFactory.addTask("tag", TagTask.class);
		taskFactory.addTask("wipe-queue", WipeQueueTask.class);
		taskFactory.addTask("wipe-report", WipeReportTask.class);
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
			new HelpTask().run();
			return;
		}

		final String command = args[0];
		final DefaultTask task;

		try {
			task = taskFactory.getTask(command);
		} catch (IllegalArgumentException e) {
			System.err.println(String.format("Sorry, \"%s\" is not an Extract command.", command));
			System.exit(1);
			return;
		}

		final Options options = new CommonsTransformer().apply(task.options());
		final CommandLineParser parser = new DefaultParser();
		final CommandLine line;
		final ProgressBar progressBar;

		try {
			line = parser.parse(options, Arrays.copyOfRange(args, 1, args.length));
		} catch (UnrecognizedOptionException e) {
			System.err.println(e.getMessage());
			System.exit(1);
			return;
		}

		if (task instanceof Monitorable) {
			progressBar = new ProgressBar(command, 0);

			((Monitorable) task).addListener(new ConsoleProgressListener(progressBar));
			progressBar.start();
		} else {
			progressBar = null;
		}

		if (line.getArgs().length > 0) {
			task.run(line.getArgs());
		} else {
			task.run();
		}

		if (null != progressBar) {
			progressBar.stop();
		}
	}
}
