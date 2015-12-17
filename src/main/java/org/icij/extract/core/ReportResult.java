package org.icij.extract.core;

import java.util.Map;
import java.util.HashMap;

/**
 * Log for the extraction result of a file.
 *
 * @since 1.0.0-beta
 */
public enum ReportResult {
	SUCCEEDED(0),
	NOT_FOUND(1),
	NOT_READ(2),
	NOT_DECRYPTED(3),
	NOT_PARSED(4),
	NOT_CLEAR(9),
	NOT_SAVED(10);

	private final int value;

	private static final Map<Integer, ReportResult> lookup = new HashMap<Integer, ReportResult>();

	static {
		for (ReportResult result: ReportResult.values()) {
			lookup.put(result.getValue(), result);
		}
	}

	public static final ReportResult get(final Number value) {
		return get(new Integer(value.intValue()));
	}

	public static final ReportResult get(final Integer value) {
		return lookup.get(value);
	}

	ReportResult(final int value) {
		this.value = value;
	}

	public int getValue() {
		return value;
	}
}
