package org.icij.extract.core;

import java.util.Map;
import java.util.Set;
import java.util.HashSet;

import java.io.Reader;
import java.io.IOException;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import java.util.logging.Logger;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;

/**
 * Base class for Spewer superclasses that write text output from a {@link ParsingReader} to specific endpoints.
 *
 * @since 1.0.0-beta
 */
public abstract class Spewer {

	public static final String META_PARENT_PATH = "Parent-Path";
	public static final String META_CONTENT_BASE_TYPE = "Content-Base-Type";

	protected final Logger logger;

	protected Path outputBase = null;
	protected boolean outputMetadata = false;
	protected Charset outputEncoding = StandardCharsets.UTF_8;
	protected Map<String, String> tags = null;

	public Spewer(final Logger logger) {
		this.logger = logger;
	}

	public abstract void write(final Path file, final Metadata metadata, final Reader reader,
		final Charset outputEncoding) throws IOException;

	public void write(final Path file, final Metadata metadata, final Reader reader)
		throws IOException {
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
		setOutputBase(Paths.get(outputBase));
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

	public void setTags(final Map<String, String> tags) {
		this.tags = tags;
	}

	public void finish() throws IOException {
		logger.info("Spewer finishing pending jobs.");
	}

	protected void filterMetadata(final Path file, final Metadata metadata) {
		final Set<String> baseTypes = new HashSet<>();
		final Path parent = file.getParent();

		// Add the parent path.
		if (null != parent) {
			metadata.set(META_PARENT_PATH, parent.toString());
		}

		// Add the base type. De-duplicated.
		for (String type : metadata.getValues(Metadata.CONTENT_TYPE)) {
			MediaType mediaType = MediaType.parse(type);
			if (null == mediaType) {
				logger.warning(String.format("Content type could not be parsed: \"%s\". Was: \"%s\".",
					file, type));
				continue;
			}

			String baseType = mediaType.getBaseType().toString();
			if (baseTypes.add(baseType)) {
				metadata.add(META_CONTENT_BASE_TYPE, baseType);
			}
		}
	}
}
