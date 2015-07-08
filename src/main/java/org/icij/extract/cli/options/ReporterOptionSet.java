package org.icij.extract.cli.options;

import org.apache.commons.cli.Option;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public class ReporterOptionSet extends OptionSet {

	public ReporterOptionSet() {
		super(Option.builder()
				.desc("The name of the report. Defaults to \"extract\".")
				.longOpt("report-name")
				.hasArg()
				.argName("name")
				.build(),

			Option.builder("r")
				.desc("Set the reporter backend type. A reporter is used to skip files that have already been extracted and outputted successfully. For now, the only valid value is \"redis\".")
				.longOpt("reporter")
				.hasArg()
				.argName("type")
				.build());
	}
}
