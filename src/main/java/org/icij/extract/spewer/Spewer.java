package org.icij.extract.spewer;

import java.io.Reader;
import java.io.Writer;
import java.io.IOException;
import java.io.StringWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import java.util.*;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import org.apache.tika.metadata.*;
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
@Option(name = "isoDates", description = "Attempt to parse dates and convert them to ISO 8601 UTC format. On by " +
		"default.")
public abstract class Spewer implements AutoCloseable {

	boolean outputMetadata = true;
	private boolean isoDates = true;

	private Charset outputEncoding = StandardCharsets.UTF_8;
	final Map<String, String> tags = new HashMap<>();
	protected final FieldNames fields;

	public Spewer(final FieldNames fields) {
		this.fields = fields;
	}

	public Spewer configure(final Options<String> options) {
		options.get("outputMetadata").parse().asBoolean().ifPresent(this::outputMetadata);
		options.get("isoDates").parse().asBoolean().ifPresent(this::isoDates);
		options.get("charset").value(Charset::forName).ifPresent(this::setOutputEncoding);
		options.get("tag").values().forEach(this::setTag);

		return this;
	}

	public abstract void write(final Document document, final Reader reader) throws IOException;

	public abstract void writeMetadata(final Document document) throws IOException;

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

	public void isoDates(final boolean isoDates) {
		this.isoDates = isoDates;
	}

	public boolean isoDates() {
		return isoDates;
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

	public static String toString(final Reader reader) throws IOException {
		final StringWriter writer = new StringWriter(4096);

		copy(reader, writer);
		return writer.toString();
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

	void applyMetadata(final Metadata metadata, final PairConsumer single, final PairArrayConsumer multiple) throws
			SpewerException {
		try {
			for (String name : metadata.names()) {
				boolean isMultivalued = metadata.isMultiValued(name);

				// Bad HTML files can have many titles. Ignore all but the first.
				if (isMultivalued && (name.equals(TikaCoreProperties.TITLE.getName()) || name.equals(Metadata.TITLE))) {
					isMultivalued = false;
				}

				if (isMultivalued) {
					applyMetadata(metadata, name, multiple);
				} else {
					applyMetadata(metadata, name, single);
				}
			}
		} catch (IOException e) {
			throw new SpewerException("Error while writing metadata.", e);
		}
	}

	private void applyMetadata(final Metadata metadata, final String name, final PairArrayConsumer consumer) throws
			IOException {
		String[] values = metadata.getValues(name);
		Stream<String> stream = Arrays.stream(values);

		// Remove empty values.
		stream = stream.filter(value -> null != value && !value.isEmpty());

		// Remove duplicate content types (Tika seems to add these sometimes, especially for RTF files)..
		if (values.length > 1 && name.equals(Metadata.CONTENT_TYPE)) {
			stream = stream.distinct();
		}

		values = stream.toArray(String[]::new);
		if (values.length > 0) {
			consumer.accept(fields.forMetadata(name), values);
		}
	}

	private void applyMetadata(final Metadata metadata, final String name, final PairConsumer consumer) throws
			IOException {
		final String value = metadata.get(name);

		if (null == value || value.isEmpty()) {
			return;
		}

		consumer.accept(fields.forMetadata(name), value);

		// Add a separate field containing the ISO 8601 date.
		final Property property = isDate(name);

		if (isoDates && null != property) {
			final Date isoDate = metadata.getDate(property);

			if (null != isoDate) {
				consumer.accept(fields.forMetadataISODate(name), isoDate.toInstant().toString());
			} else {
				throw new SpewerException(String.format("Unable to parse date \"%s\" from field \"%s\" for ISO 8601 " +
						"formatting.", value, name));
			}
		}
	}

	private Property isDate(final String name) {
		Property property = null;

		for (Property dateProperty : dateProperties) {
			if (dateProperty.getName().equals(name)) {
				property = dateProperty;
				break;
			}
		}

		return property;
	}

	private static final List<Property> dateProperties = Arrays.asList(
			DublinCore.DATE,
			DublinCore.CREATED,
			DublinCore.MODIFIED,
			Office.CREATION_DATE,
			Office.SAVE_DATE,
			Office.PRINT_DATE,
			MSOffice.CREATION_DATE,
			MSOffice.LAST_SAVED,
			MSOffice.LAST_PRINTED,
			PDF.DOC_INFO_CREATED,
			PDF.DOC_INFO_MODIFICATION_DATE,
			TIFF.ORIGINAL_DATE,
			Metadata.DATE,
			//Metadata.MODIFIED,
			HttpHeaders.LAST_MODIFIED);

	@FunctionalInterface
	interface PairConsumer {

		void accept(final String name, final String value) throws IOException;
	}

	@FunctionalInterface
	interface PairArrayConsumer {

		void accept(final String name, final String[] values) throws IOException;
	}
}
