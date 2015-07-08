package org.icij.extract.cli.options;

import org.apache.commons.cli.Option;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public class QueueOptionSet extends OptionSet {

	public QueueOptionSet() {
		super(Option.builder()
				.desc("The name of the queue. Defaults to \"extract\".")
				.longOpt("queue-name")
				.hasArg()
				.argName("name")
				.build(),

			Option.builder("q")
				.desc("Set the queue backend type. For now, the only valid value is \"redis\".")
				.longOpt("queue")
				.hasArg()
				.argName("type")
				.build());
	}
}
