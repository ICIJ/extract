package org.icij.extract.report;

import org.icij.extract.document.TikaDocument;
import org.icij.extract.document.DocumentFactory;

import org.icij.extract.mysql.DataSourceFactory;
import org.icij.task.Options;
import org.icij.task.annotation.Option;
import org.icij.task.annotation.OptionsClass;

/**
 * Factory methods for creating {@link ReportMap} objects.
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @since 1.0.0-beta
 */
@Option(name = "reportType", description = "Set the report backend type. Either \"redis\" or \"mysql\".",
		parameter = "type", code = "r")
@OptionsClass(DocumentFactory.class)
@OptionsClass(DataSourceFactory.class)
@OptionsClass(RedisReportMap.class)
@OptionsClass(MySQLReportMap.class)
public class ReportMapFactory {

	private ReportMapType type = null;
	private Options<String> options = null;
	private DocumentFactory documentFactory = null;
	private DataSourceFactory dataSourceFactory = null;

	/**
	 * Prefers an in-local-memory map by default.
	 *
	 * @param options options for creating the queue
	 */
	public ReportMapFactory(final Options<String> options) {
		type = options.get("reportType").parse().asEnum(ReportMapType::parse).orElse(ReportMapType.HASH);
		this.options = options;
	}

	/**
	 * Set the documentFactory used for creating {@link TikaDocument} objects from the report.
	 *
	 * If none is set, a default instance will be created using the given options.
	 *
	 * @param factory the documentFactory to use
	 * @return chainable documentFactory
	 */
	public ReportMapFactory withDocumentFactory(final DocumentFactory factory) {
		this.documentFactory = factory;
		return this;
	}

	/**
	 * Set the data source factory for SQL-backed reports.
	 *
	 * If none is set, a default instance will be created using the given options.
	 *
	 * @param dataSourceFactory the data source factory to use
	 * @return chainable documentFactory
	 */
	public ReportMapFactory withDataSourceFactory(final DataSourceFactory dataSourceFactory) {
		this.dataSourceFactory = dataSourceFactory;
		return this;
	}

	/**
	 * Create a new report map from the given arguments.
	 *
	 * @return a new report or {@code null} if no type is specified
	 */
	public ReportMap create() {
		if (ReportMapType.HASH == type) {
			return new HashMapReportMap();
		}

		return createShared();
	}

	/**
	 * Create a new shared report map from options.
	 *
	 * @return a new server-backed report
	 * @throws IllegalArgumentException if the given options do not contain a valid shared report type
	 */
	public ReportMap createShared() throws IllegalArgumentException {
		if (null == documentFactory) {
			documentFactory = new DocumentFactory().configure(options);
		}

		if (ReportMapType.REDIS == type) {
			return new RedisReportMap(documentFactory, options);
		}

		if (ReportMapType.MYSQL == type) {
			if (null == dataSourceFactory) {
				dataSourceFactory = new DataSourceFactory(options);
			}

			return new MySQLReportMap(dataSourceFactory.get(), documentFactory, options);
		}

		throw new IllegalArgumentException(String.format("\"%s\" is not a valid shared report type.", type));
	}
}
