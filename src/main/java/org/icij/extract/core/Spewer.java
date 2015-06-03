package org.icij.extract.core;

import java.io.IOException;

import java.nio.file.Path;
import java.nio.charset.Charset;

import java.util.logging.Logger;

import org.apache.tika.parser.ParsingReader;
import org.apache.tika.exception.TikaException;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public abstract class Spewer {

	protected final Logger logger;

	private String outputBase = null;

	public Spewer(Logger logger) {
		this.logger = logger;
	}

	public abstract void write(Path file, ParsingReader reader, Charset outputEncoding) throws IOException, TikaException, SpewerException;

	public void setOutputBase(String outputBase) {
		this.outputBase = outputBase;
	}

	public String filterOutputPath(String path) {
		if (path.startsWith(outputBase)) {
			return path.substring(outputBase.length());
		}

		return path;
	}

	public void finish() throws IOException {
		logger.info("Spewer finishing pending jobs.");
	}
}
