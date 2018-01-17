package org.icij.extract.cli.tasks;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.io.FileUtils;
import org.icij.extract.cli.CommonsTransformer;
import org.icij.extract.cli.Main;
import org.icij.task.DefaultTask;
import org.icij.task.Options;
import org.icij.task.StringOptionParser;

import javax.imageio.ImageIO;
import java.util.Arrays;
import java.util.Set;

public class HelpTask extends DefaultTask<Void> {

	private final String footer = String.format("%nExtract will use up to %s of memory on this machine.%n%n" +
			"Please report issues at: https://github.com/ICIJ/extract/issues.", FileUtils.byteCountToDisplaySize
			(Runtime.getRuntime().maxMemory()));

	@Override
	public Void run(final String[] args) throws Exception {
		final String command = args[0];
		final DefaultTask<Object> task = Main.taskFactory.getTask(command);
		final Options<String> options = task.options();
		final HelpFormatter formatter = new HelpFormatter();

		options.add("options", StringOptionParser::new)
				.describe("Load options from a Java properties file.")
				.parameter("path");

		formatter.printHelp(String.format("extract %s", command), "\n" + task.description() + "\n\n",
				new CommonsTransformer().apply(options), footer);
		return null;
	}

	@Override
	public Void run() throws Exception {
		ImageIO.scanForPlugins();

		final HelpFormatter formatter = new HelpFormatter();
		final Set<String> tasks = Main.taskFactory.listNames();
		final String[] imageFormats = Arrays.stream(ImageIO.getReaderFormatNames())
				.map(String::toLowerCase)
				.distinct()
				.toArray(String[]::new);

		String header = String.format("%nA cross-platform tool for distributed content-extraction " +
				"by the data team at the International Consortium of Investigative Journalists.%n%n" +
				"\033[1mCommands\033[0m%n%n %s%n%n\033[1mAdditional Image Formats\033[0m%n%n %s", String.join("\n ",
				tasks), String.join("\n ", (CharSequence[]) imageFormats));

		formatter.printHelp(String.format("\033[1mextract\033[0m [command] [options]%n" +
				"%s\033[1mextract\033[0m help%n" +
				"%s\033[1mextract\033[0m version", formatter.getSyntaxPrefix(), formatter
				.getSyntaxPrefix()), header, new org.apache.commons.cli.Options(), footer, false);
		return null;
	}
}
