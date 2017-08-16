package org.icij.extract.spewer;

import java.io.Reader;
import java.io.Writer;
import java.io.Serializable;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.OutputStream;
import java.io.StringWriter;

import java.nio.file.Path;
import java.util.Map;
import java.util.HashMap;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.icij.extract.document.Document;
import org.icij.extract.parser.ParsingReader;
import org.icij.task.Options;
import org.icij.task.annotation.Option;

/**
 * Base class for {@linkplain Spewer} superclasses that write text output from a {@link ParsingReader} to specific
 * endpoints.
 *
 * @since 1.0.0-beta
 */
@Option(name = "outputMetadata", description = "Output metadata along with extracted text. For the " +
		"\"file\" output type, a corresponding JSON file is created for every input file. With indexes, metadata " +
		"fields are set using an optional prefix. On by default.")
@Option(name = "tag", description = "Set the given field to a corresponding value on each document output.",
		parameter = "name-value-pair")
@Option(name = "charset", description = "Set the output encoding for text and document attributes. Defaults to UTF-8.",
		parameter = "name")
public abstract class Spewer implements AutoCloseable, Serializable {

	private static final long serialVersionUID = 5169670165236652447L;

	boolean outputMetadata = true;

	private Charset outputEncoding = StandardCharsets.UTF_8;
	final Map<String, String> tags = new HashMap<>();
	protected final FieldNames fields;

	public Spewer(final FieldNames fields) {
		this.fields = fields;
	}

	public Spewer configure(final Options<String> options) {
		options.get("outputMetadata").parse().asBoolean().ifPresent(this::outputMetadata);
		options.get("charset").value(Charset::forName).ifPresent(this::setOutputEncoding);
		options.get("tag").values().forEach(this::setTag);

		return this;
	}

	public abstract void write(final Document document, final Reader reader) throws IOException;

	public abstract void writeMetadata(final Document document) throws IOException;

	public Document[] write(final Path path) throws IOException, ClassNotFoundException {
		throw new UnsupportedOperationException("Not implemented.");
	}

	public FieldNames getFields() {
		return fields;
	}

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

	protected void copy(final Reader input, final OutputStream output) throws IOException {
		copy(input, new OutputStreamWriter(output, outputEncoding));
	}

	public static void copy(final Reader input, final Writer output) throws IOException {
		final char[] buffer = new char[1024];
		int n;

		while (-1 != (n = input.read(buffer))) {
			output.write(buffer, 0, n);
		}

		output.flush();
	}

	public static String toString(final Reader reader) throws IOException {
		final StringWriter writer = new StringWriter(4096);

		copy(reader, writer);
		return writer.toString();
	}
}
