package org.icij.extract.core;

import java.io.IOException;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
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
