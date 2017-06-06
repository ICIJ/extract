package org.icij.extract.report;

import org.icij.extract.document.Document;
import org.icij.extract.extractor.ExtractionStatus;
import org.icij.kaxxa.sql.concurrent.SQLCodec;
import org.icij.task.Options;
import org.icij.task.annotation.Option;

import java.io.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

@Option(name = "reportIdKey", description = "For reports tables that have an ID column, specify the column name. This" +
		" will then be used as the unique key.", parameter = "name")
@Option(name = "reportForeignIdKey", description = "For reports that have a foreign ID column, specify the column " +
		"name.", parameter = "name")
@Option(name = "reportPathKey", description = "The report table key for storing the document path.", parameter = "name")
@Option(name = "reportStatusKey", description = "The table key for storing the report status.", parameter = "name")
@Option(name = "reportExceptionKey", description = "The table key for storing processing exceptions.", parameter =
		"name")
@Option(name = "reportSuccessStatus", description = "The status for a successfully extracted file.", parameter =
		"value")
@Option(name = "reportFailureStatus", description = "A general failure status value to use instead of the more " +
		"specific values.", parameter = "value")
class SQLReportCodec implements SQLCodec<Report> {

	private final String idKey;
	private final String foreignIdKey;
	private final String pathKey;
	private final String statusKey;
	private final String exceptionKey;
	private final String successStatus;
	private final String failureStatus;

	SQLReportCodec(final Options<String> options) {
		this.idKey = options.get("reportIdKey").value().orElse(null);
		this.foreignIdKey = options.get("reportForeignIdKey").value().orElse(null);
		this.pathKey = options.get("reportPathKey").value().orElse("path");
		this.statusKey = options.get("reportStatusKey").value().orElse("extraction_status");
		this.exceptionKey = options.get("reportExceptionKey").value().orElse("exception");
		this.successStatus = options.get("reportSuccessStatus").value().orElse(null);
		this.failureStatus = options.get("reportFailureStatus").value().orElse(null);
	}

	@Override
	public Map<String, Object> encodeKey(final Object o) {
		final Document document = (Document) o;
		final Map<String, Object> map = new HashMap<>();

		if (null != idKey) {
			map.put(idKey, document.getId());
		}

		if (null != foreignIdKey) {
			map.put(foreignIdKey, document.getForeignId());
		}

		map.put(pathKey, document.getPath().toString());
		return map;
	}

	@Override
	public Map<String, Object> encodeValue(final Object o) {
		final Report report = (Report) o;
		final ExtractionStatus status = report.getStatus();
		final Map<String, Object> map = new HashMap<>();

		if (successStatus != null && status == ExtractionStatus.SUCCESS) {
			map.put(statusKey, successStatus);
		} else if (failureStatus != null && status.getCode() > 0) {
			map.put(statusKey, failureStatus);
		} else {
			map.put(statusKey, status.toString());
		}

		if (null != exceptionKey && report.getException().isPresent()) {
			try (final ByteArrayOutputStream bo = new ByteArrayOutputStream(1024);
			     final ObjectOutputStream so = new ObjectOutputStream(bo)) {

				so.writeObject(report.getException());
				so.flush();
				map.put(exceptionKey, bo.toString());
			} catch (final IOException ignored) {

			}
		}

		return map;
	}

	@Override
	public Report decodeValue(final ResultSet rs) throws SQLException {
		final String status = rs.getString(statusKey);
		Exception exception = null;

		final byte[] exceptionBytes = rs.getBytes(exceptionKey);

		if (null != exceptionBytes && exceptionBytes.length > 0) {
			try (final ObjectInputStream si = new ObjectInputStream(new ByteArrayInputStream(exceptionBytes))) {
				exception = (Exception) si.readObject();
			} catch (final IOException | ClassNotFoundException e) {
				exception = null;
			}
		}

		// Assume that the status is stored as an ENUM in the table, so decode as the ExtractionStatus name
		// rather than integer value. This is different to Redis codec, where integer values are stored to save
		// memory. MySQL converts ENUM strings to integers internally, so no such manual device is needed there.
		if (null == status) {
			return null;
		}

		if (null != successStatus && status.equals(successStatus)) {
			return new Report(ExtractionStatus.SUCCESS);
		}

		if (null != failureStatus && status.equals(failureStatus)) {
			return new Report(ExtractionStatus.FAILURE_UNKNOWN, exception);
		}

		// Return null if the value can't be decoded into an enum. This allows arbitrary values to be stored in
		// the status column.
		try {
			return new Report(ExtractionStatus.valueOf(status), exception);
		} catch (IllegalArgumentException e) {
			return null;
		}
	}
}
