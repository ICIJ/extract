package org.icij.extract.core;

import java.util.Map;
import java.util.HashMap;

/**
 * Log for the extraction result of a file.
 *
 * @since 1.0.0-beta
 */
public enum ExtractionResult {
	SUCCEEDED(0),
	NOT_FOUND(1),
	NOT_READ(2),
	NOT_DECRYPTED(3),
	NOT_PARSED(4),
	EXCLUDED(5),
	NOT_CLEAR(9),
	NOT_SAVED(10),;

	private final int value;

	private static final Map<Integer, ExtractionResult> lookup = new HashMap<>();

	static {
		for (ExtractionResult result: ExtractionResult.values()) {
			lookup.put(result.getValue(), result);
		}
	}

	public static ExtractionResult get(final Number value) {
		return get(value.intValue());
	}

	public static ExtractionResult get(final Integer value) {
		return lookup.get(value);
	}

	public static ExtractionResult get(final String value) {
		return get(Integer.valueOf(value));
	}

	ExtractionResult(final int value) {
		this.value = value;
	}

	public int getValue() {
		return value;
	}
}
