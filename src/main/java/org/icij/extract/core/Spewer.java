package org.icij.extract.core;

import java.io.IOException;

import java.nio.file.Path;
import java.nio.charset.Charset;

import java.util.logging.Logger;

import org.apache.tika.parser.ParsingReader;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public abstract class Spewer {

	protected final Logger logger;

	public Spewer(Logger logger) {
		this.logger = logger;
	}

	public abstract void write(Path file, ParsingReader reader, Charset outputEncoding) throws IOException;

	public void finish() throws IOException {
		logger.info("Spewer finishing pending jobs.");
	}
}
