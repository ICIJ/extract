package org.icij.extract.cli;

import org.icij.extract.core.*;

import java.util.Locale;
import java.util.logging.Logger;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public enum Command {
	QUEUE(QueueCli.class),
	WIPE_QUEUE(WipeQueueCli.class),
	WIPE_REPORT(WipeReportCli.class),
	DUMP_QUEUE(DumpQueueCli.class),
	DUMP_REPORT(DumpReportCli.class),
	LOAD_QUEUE(LoadQueueCli.class),
	SPEW(SpewCli.class),
	SOLR_COMMIT(SolrCommitCli.class),
	SOLR_ROLLBACK(SolrRollbackCli.class),
	SOLR_DELETE(SolrDeleteCli.class),
	SOLR_COPY(SolrCopyCli.class);

	private final Class<?> klass;

	private Command(Class<?> klass) {
		this.klass = klass;
	}

	public String toString() {
		return name().toLowerCase(Locale.ROOT).replace('_', '-');
	}

	public Cli createCli(Logger logger) {
		try {
			return (Cli) klass.getDeclaredConstructor(Logger.class).newInstance(logger);
		} catch (Throwable e) {
			throw new RuntimeException("Unexpected exception while constructing CLI.", e);
		}
	}

	public static Command parse(String command) throws IllegalArgumentException {
		try {
			return fromString(command);
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException(String.format("\"%s\" is not a valid command.", command));
		}
	}

	public static Command fromString(String command) {
		return valueOf(command.toUpperCase(Locale.ROOT).replace('-', '_'));
	}
};
