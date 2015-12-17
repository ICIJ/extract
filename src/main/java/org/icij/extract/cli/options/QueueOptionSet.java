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

			Option.builder()
				.desc("The size of the internal file path buffer used by the queue.")
				.longOpt("queue-buffer")
				.hasArg()
				.argName("size")
				.type(Number.class)
				.build(),

			Option.builder("q")
				.desc("Set the queue backend type. For now, the only valid values are \"redis\" and \"array\" (the default).")
				.longOpt("queue")
				.hasArg()
				.argName("type")
				.build());
	}
}
