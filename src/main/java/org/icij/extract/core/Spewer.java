package org.icij.extract.core;

import java.io.IOException;

import java.nio.file.Path;
import java.nio.file.FileSystems;
import java.nio.charset.Charset;

import java.util.logging.Logger;

import org.apache.tika.exception.TikaException;

/**
 * Base class for Spewer superclasses that write text output from a {@link ParsingReader} to specific endpoints.
 *
 * @since 1.0.0-beta
 */
public abstract class Spewer {

	protected final Logger logger;

	private Path outputBase = null;

	public Spewer(Logger logger) {
		this.logger = logger;
	}

	public abstract void write(Path file, ParsingReader reader, Charset outputEncoding) throws IOException, TikaException, SpewerException;

	public void setOutputBase(Path outputBase) {
		this.outputBase = outputBase;
	}

	public void setOutputBase(String outputBase) {
		setOutputBase(FileSystems.getDefault().getPath(outputBase));
	}

	public Path filterOutputPath(Path file) {
		if (null != outputBase && file.startsWith(outputBase)) {
			return file.subpath(outputBase.getNameCount(), file.getNameCount());
		}

		return file;
	}

	public void finish() throws IOException {
		logger.info("Spewer finishing pending jobs.");
	}
}
