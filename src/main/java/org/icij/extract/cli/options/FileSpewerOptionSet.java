package org.icij.extract.cli.options;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.CommandLine;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public class FileSpewerOptionSet extends OptionSet {

	public FileSpewerOptionSet() {
		super(Option.builder()
				.desc("Directory to output extracted text. Defaults to the current directory.")
				.longOpt("file-output-directory")
				.hasArg()
				.argName("path")
				.build());
	}
}
