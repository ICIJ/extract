package org.icij.extract.core;

import java.io.IOException;

/**
 * The Exception thrown by {@link Spewer Spewers} when writing fails due to an
 * error with the endpoint or output stream. This helps distinguish between
 * exceptions thrown by the {@link ParsingReader} and those thrown by the {@link Spewer}.
 *
 * @since 1.0.0-beta
 */
public class SpewerException extends IOException {

	public SpewerException(String message) {
		super(message);
	}

	public SpewerException(String message, Throwable cause) {
		super(message, cause);
	}
}
