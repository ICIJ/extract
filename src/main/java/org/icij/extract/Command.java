package org.icij.extract;

import java.util.logging.Logger;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public enum Command {
	QUEUE {
		public Cli createCli(Logger logger) {
			return new QueueCli(logger);
		}
	},

	WIPE_QUEUE {
		public Cli createCli(Logger logger) {
			return new WipeQueueCli(logger);
		}
	},

	EXTRACT {
		public Cli createCli(Logger logger) {
			return new ExtractCli(logger);
		}
	};

	public String toString() {
		return name().toLowerCase().replace('_', '-');
	}

	public abstract Cli createCli(Logger logger);

	public static final Command parse(String command) throws IllegalArgumentException {
		try {
			return fromString(command);
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException(String.format("\"%s\" is not a valid command.", command));
		}
	}

	public static final Command fromString(String command) {
		return valueOf(command.toUpperCase().replace('-', '_'));
	}
};
