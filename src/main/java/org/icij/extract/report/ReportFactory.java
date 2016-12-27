package org.icij.extract.report;

import org.icij.extract.document.Document;
import org.icij.extract.document.DocumentFactory;

import org.icij.task.Options;
import org.icij.task.annotation.Option;
import org.icij.task.annotation.OptionsClass;

/**
 * Factory methods for creating {@link Report} objects.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
@Option(name = "reportType", description = "Set the report backend type. Either \"redis\" or \"mysql\".",
		parameter = "type", code = "r")
@OptionsClass(RedisReport.class)
@OptionsClass(MySQLReport.class)
public class ReportFactory {

	private ReportType type = null;
	private Options<String> options = null;
	private DocumentFactory factory = null;

	/**
	 * Prefers an in-local-memory map by default.
	 *
	 * @param options options for creating the queue
	 */
	public ReportFactory(final Options<String> options) {
		type = options.get("reportType").parse().asEnum(ReportType::parse).orElse(ReportType.HASH);
		this.options = options;
	}

	/**
	 * Set the factory used for creating {@link Document} objects from the report.
	 *
	 * If none is set, a default instance will be created using the given options.
	 *
	 * @param factory the factory to use
	 * @return chainable factory
	 */
	public ReportFactory withDocumentFactory(final DocumentFactory factory) {
		this.factory = factory;
		return this;
	}

	/**
	 * Create a new report from the given arguments.
	 *
	 * @return a new report or {@code null} if no type is specified
	 */
	public Report create() {
		if (ReportType.HASH == type) {
			return new HashMapReport();
		}

		return createShared();
	}

	/**
	 * Create a new Redis-backed report from commandline parameters.
	 *
	 * @return a new Redis-backed report
	 * @throws IllegalArgumentException if the given options do not contain a valid shared report type
	 */
	public Report createShared() throws IllegalArgumentException {
		if (null == factory) {
			factory = new DocumentFactory().configure(options);
		}

		if (ReportType.REDIS == type) {
			return new RedisReport(factory, options);
		}

		if (ReportType.MYSQL == type) {
			return new MySQLReport(options);
		}

		throw new IllegalArgumentException(String.format("\"%s\" is not a valid shared report type.", type));
	}
}
