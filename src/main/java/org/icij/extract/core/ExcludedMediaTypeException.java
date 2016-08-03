package org.icij.extract.core;

import org.apache.tika.exception.TikaException;

/**
 * The exception thrown when a file could not be parsed because the parser
 * that would otherwise handle it was excluded.
 *
 * @since 1.0.0-beta
 */
public class ExcludedMediaTypeException extends TikaException {

	private static final long serialVersionUID = 7867649548995177839L;

	public ExcludedMediaTypeException(String message) {
		super(message);
	}

	public ExcludedMediaTypeException(String message, Throwable cause) {
		super(message, cause);
	}
}
