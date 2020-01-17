package org.icij.extract.report;

import org.icij.extract.extractor.ExtractionStatus;

import java.util.Objects;
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

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof Report)) return false;
		Report report = (Report) o;
		if (!getException().isPresent()) {
			return status == report.status;
		} else {
			return report.getException().isPresent() && status == report.status &&
					exception.getClass().equals(report.exception.getClass()) &&
					exception.getMessage().equals(report.exception.getMessage());
		}
	}

	@Override
	public int hashCode() {
		return exception == null ?
				Objects.hash(status):
				Objects.hash(status, exception.getClass(), exception.getMessage());
	}
}
