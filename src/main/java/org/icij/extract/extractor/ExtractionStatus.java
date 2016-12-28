package org.icij.extract.extractor;

import org.icij.extract.spewer.SpewerException;

import java.util.Map;
import java.util.HashMap;

/**
 * Status for the extraction result of a file.
 *
 * @since 1.0.0-beta
 */
public enum ExtractionStatus {
	SUCCEEDED(0),
	NOT_FOUND(1),
	NOT_READ(2),
	NOT_DECRYPTED(3),
	NOT_PARSED(4),
	EXCLUDED(5),
	UNKNOWN_ERROR(9),
	NOT_SAVED(10),;

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
		return parse(Integer.valueOf(value));
	}

	ExtractionStatus(final int code) {
		this.code = code;
	}

	public int getCode() {
		return code;
	}
}
