package org.icij.extract.core;

import java.io.Reader;
import java.io.IOException;

import java.nio.file.Path;
import java.nio.file.FileSystems;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import java.util.logging.Logger;

import org.apache.tika.metadata.Metadata;

/**
 * Base class for Spewer superclasses that write text output from a {@link ParsingReader} to specific endpoints.
 *
 * @since 1.0.0-beta
 */
public abstract class Spewer {

	protected final Logger logger;

	protected Path outputBase = null;
	protected boolean outputMetadata = false;
	protected Charset outputEncoding = StandardCharsets.UTF_8;

	public Spewer(final Logger logger) {
		this.logger = logger;
	}

	public abstract void write(final Path file, final Metadata metadata, final Reader reader,
		final Charset outputEncoding) throws IOException, SpewerException;

	public void write(final Path file, final Metadata metadata, final Reader reader)
		throws IOException, SpewerException {
		write(file, metadata, reader, outputEncoding);
	}

	public void setOutputEncoding(final Charset outputEncoding) {
		this.outputEncoding = outputEncoding;
	}

	public void setOutputEncoding(final String outputEncoding) {
		setOutputEncoding(Charset.forName(outputEncoding));
	}

	public void setOutputBase(final Path outputBase) {
		this.outputBase = outputBase;
	}

	public void setOutputBase(final String outputBase) {
		setOutputBase(FileSystems.getDefault().getPath(outputBase));
	}

	public Path getOutputBase() {
		return outputBase;
	}

	public void outputMetadata(final boolean outputMetadata) {
		this.outputMetadata = outputMetadata;
	}

	public boolean outputMetadata() {
		return outputMetadata;
	}

	public Path filterOutputPath(final Path file) {
		if (null != outputBase && file.startsWith(outputBase)) {
			return file.subpath(outputBase.getNameCount(), file.getNameCount());
		} else {
			return file;
		}
	}

	public void finish() throws IOException {
		logger.info("Spewer finishing pending jobs.");
	}
}
