package org.icij.extract.cli.options;

import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.CommandLine;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public class RedisOptionSet extends OptionSet {

	public RedisOptionSet() {
		super(Option.builder()
				.desc("Set the Redis backend address. Defaults to 127.0.0.1:6379.")
				.longOpt("redis-address")
				.hasArg()
				.argName("address")
				.build());
	}
}
