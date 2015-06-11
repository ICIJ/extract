package org.icij.extract.core;

import org.apache.tika.exception.TikaException;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public class ExcludedMediaTypeException extends TikaException {

	public ExcludedMediaTypeException(String message) {
		super(message);
	}

	public ExcludedMediaTypeException(String message, Throwable cause) {
		super(message, cause);
	}
}
