package org.icij.extract.spewer;

import org.icij.extract.parser.ParsingReader;

import java.io.IOException;

/**
 * The Exception thrown by {@link Spewer Spewers} when writing fails due to an
 * error with the endpoint or output stream. This helps distinguish between
 * exceptions thrown by the {@link ParsingReader} and those thrown by the {@link Spewer}.
 *
 * @since 1.0.0-beta
 */
public class SpewerException extends IOException {

	private static final long serialVersionUID = 3981185100146162422L;

	SpewerException(String message) {
		super(message);
	}

	SpewerException(String message, Throwable cause) {
		super(message, cause);
	}
}
