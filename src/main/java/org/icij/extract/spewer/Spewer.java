package org.icij.extract.spewer;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import java.io.Reader;
import java.io.IOException;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

import org.apache.tika.metadata.Metadata;
import org.icij.extract.document.Document;
import org.icij.extract.parser.ParsingReader;
import org.icij.task.Options;

/**
 * Base class for {@linkplain Spewer} superclasses that write text output from a {@link ParsingReader} to specific
 * endpoints.
 *
 * @since 1.0.0-beta
 */
public abstract class Spewer implements AutoCloseable {

	private static final Pattern fieldName = Pattern.compile("[^A-Za-z0-9_:]");

	String normalizeMetadataName(final String name) {
		String normalizedName = fieldName.matcher(name).replaceAll("_").toLowerCase(Locale.ROOT);
		final String prefix = fields.forMetadataPrefix();

		if (null != prefix) {
			normalizedName = prefix + normalizedName;
		}

		return normalizedName;
	}

	boolean outputMetadata = true;
	Charset outputEncoding = StandardCharsets.UTF_8;
	final Map<String, String> tags = new HashMap<>();
	protected final FieldNames fields;

	public Spewer(final FieldNames fields) {
		this.fields = fields;
	}

	public Spewer configure(final Options<String> options) {
		options.get("output-metadata").parse().asBoolean().ifPresent(this::outputMetadata);
		options.get("charset").value(Charset::forName).ifPresent(this::setOutputEncoding);
		options.get("tag").values().forEach(this::setTag);

		return this;
	}

	public abstract void write(final Document document, final Reader reader) throws
			IOException;

	public abstract void writeMetadata(final Document document) throws
			IOException;

	public void setOutputEncoding(final Charset outputEncoding) {
		this.outputEncoding = outputEncoding;
	}

	public Charset getOutputEncoding() {
		return outputEncoding;
	}

	public void outputMetadata(final boolean outputMetadata) {
		this.outputMetadata = outputMetadata;
	}

	public boolean outputMetadata() {
		return outputMetadata;
	}

	public void setTags(final Map<String, String> tags) {
		tags.forEach(this::setTag);
	}

	private void setTag(final String name, final String value) {
		tags.put(name, value);
	}

	private void setTag(final String tag) {
		final String[] pair = tag.split(":", 2);

		if (2 == pair.length) {
			setTag(pair[0], pair[1]);
		} else {
			throw new IllegalArgumentException(String.format("Invalid tag pair: \"%s\".", tag));
		}
	}
}
