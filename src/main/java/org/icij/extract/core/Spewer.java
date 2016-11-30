package org.icij.extract.core;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.io.Reader;
import java.io.IOException;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.apache.tika.metadata.Metadata;
import org.icij.task.Options;

/**
 * Base class for Spewer superclasses that write text output from a {@link ParsingReader} to specific endpoints.
 *
 * @since 1.0.0-beta
 */
public abstract class Spewer implements AutoCloseable {

	private Path outputBase = null;

	protected boolean outputMetadata = true;
	protected Charset outputEncoding = StandardCharsets.UTF_8;
	protected Map<String, String> tags = null;

	public Spewer() {

	}

	public Spewer(final Options<String> options) {
		options.get("output-metadata").parse().asBoolean().ifPresent(this::outputMetadata);
		options.get("output-encoding").value().ifPresent(this::setOutputEncoding);
		setTags(options.get("tag").values());
	}

	public abstract void write(final Path file, final Metadata metadata, final Reader reader) throws IOException;

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

	public void setTag(final String name, final String value) {
		if (null == tags) {
			tags = new HashMap<>();
		}

		tags.put(name, value);
	}

	private void setTags(final List<String> tags) {
		for (String tag : tags) {
			String[] pair = tag.split(":", 2);

			if (2 == pair.length) {
				setTag(pair[0], pair[1]);
			} else {
				throw new IllegalArgumentException(String.format("Invalid tag pair: \"%s\".", tag));
			}
		}
	}
}
