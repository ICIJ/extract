package org.icij.extract.extractor;

import java.util.Map;
import java.util.HashMap;

/**
 * Status for the extraction result of a file.
 *
 * @since 1.0.0-beta
 */
public enum ExtractionStatus {
	SUCCESS(0),
	FAILURE_NOT_FOUND(1),
	FAILURE_UNREADABLE(2),
	FAILURE_NOT_DECRYPTED(3),
	FAILURE_NOT_PARSED(4),
	FAILURE_UNKNOWN(9),
	FAILURE_NOT_SAVED(10);

	private final int code;

	private static final Map<Integer, ExtractionStatus> lookup = new HashMap<>();

	static {
		for (ExtractionStatus result: ExtractionStatus.values()) {
			lookup.put(result.getCode(), result);
		}
	}

	public static ExtractionStatus parse(final Number value) {
		return parse(value.intValue());
	}

	public static ExtractionStatus parse(final Integer value) {
		return lookup.get(value);
	}

	public static ExtractionStatus parse(final String value) {
		try {
			return parse(Integer.valueOf(value));
		} catch (final NumberFormatException e) {
			return valueOf(value);
		}
	}

	ExtractionStatus(final int code) {
		this.code = code;
	}

	public int getCode() {
		return code;
	}
}
