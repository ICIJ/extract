package org.icij.extract.report;

import org.icij.extract.extractor.ExtractionStatus;

import java.util.Optional;

public class Report {

	private final ExtractionStatus status;
	private final Exception exception;

	public Report(final ExtractionStatus status, final Exception exception) {
		this.status = status;
		this.exception = exception;
	}

	public Report(final ExtractionStatus status) {
		this.status = status;
		this.exception = null;
	}

	public ExtractionStatus getStatus() {
		return status;
	}

	public Optional<Exception> getException() {
		return Optional.ofNullable(exception);
	}
}
