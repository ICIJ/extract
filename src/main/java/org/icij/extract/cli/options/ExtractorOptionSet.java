package org.icij.extract.cli.options;

import org.icij.extract.core.*;

import java.nio.file.Paths;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.CommandLine;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public class ExtractorOptionSet extends OptionSet {

	public ExtractorOptionSet() {
		super(Option.builder()
				.desc("Set the output format. Either \"text\" or \"HTML\". Defaults to text output.")
				.longOpt("output-format")
				.hasArg()
				.argName("format")
				.build(),

			Option.builder("e")
				.desc("Set the embed handling mode. Either \"ignore\", \"extract\" or \"embed\". When set to extract, embeds are parsed and the output is in-lined into the main output. In embed mode, embeds are not parsed but are in-lined as a data URI representation of the raw embed data. The latter mode only applies when the output format is set to HTML. Defaults to extracting.")
				.longOpt("embed-handling")
				.hasArg()
				.argName("mode")
				.build(),

			Option.builder()
				.desc("Set the language used by Tesseract. If none is specified, English is assumed. Multiple languages may be specified, separated by plus characters. Tesseract uses 3-character ISO 639-2 language codes.")
				.longOpt("ocr-language")
				.hasArg()
				.argName("language")
				.build(),

			Option.builder()
				.desc(String.format("Set the timeout for the Tesseract process to finish e.g. \"5s\" or \"1m\". " +
						"Defaults to \"%s\".", Extractor.DEFAULT_OCR_TIMEOUT))
				.longOpt("ocr-timeout")
				.hasArg()
				.argName("duration")
				.build(),

			Option.builder()
				.desc("Disable automatic OCR. On by default.")
				.longOpt("ocr-disabled")
				.build(),

			Option.builder()
				.desc("Set the working directory from which to resolve paths for files passed to the extractor.")
				.longOpt("working-directory")
				.hasArg()
				.argName("directory")
				.build());
	}

	public static void configureExtractor(final CommandLine cmd, final Extractor extractor) {
		if (cmd.hasOption("output-format")) {
			extractor.setOutputFormat(Extractor
				.OutputFormat.parse(cmd.getOptionValue("output-format")));
		}

		if (cmd.hasOption('e')) {
			extractor.setEmbedHandling(Extractor
				.EmbedHandling.parse(cmd.getOptionValue('e')));
		}

		if (cmd.hasOption("ocr-language")) {
			extractor.setOcrLanguage(cmd.getOptionValue("ocr-language"));
		}

		if (cmd.hasOption("ocr-timeout")) {
			extractor.setOcrTimeout(cmd.getOptionValue("ocr-timeout"));
		}

		if (cmd.hasOption("ocr-disabled")) {
			extractor.disableOcr();
		}

		if (cmd.hasOption("working-directory")) {
			extractor.setWorkingDirectory(Paths.get(cmd.getOptionValue("working-directory")));
		}
	}
}
