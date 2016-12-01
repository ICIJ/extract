package org.icij.extract.parser;

import org.apache.tika.exception.TikaException;

/**
 * The exception thrown when a file could not be parsed because the parser that would otherwise handle it was excluded.
 *
 * @since 1.0.0-beta
 */
public class ExcludedMediaTypeException extends TikaException {

	private static final long serialVersionUID = 7867649548995177839L;

	ExcludedMediaTypeException(final String message) {
		super(message);
	}
}
