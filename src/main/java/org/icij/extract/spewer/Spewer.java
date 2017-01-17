package org.icij.extract.spewer;

import java.io.Reader;
import java.io.Writer;
import java.io.IOException;
import java.io.StringWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.function.BiConsumer;
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

	void applyMetadata(final Metadata metadata, final MetadataValueConsumer single, final MetadataValuesConsumer
			multiple) throws SpewerException {
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

	private void applyMetadata(final Metadata metadata, final String name, final MetadataValuesConsumer consumer) throws
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

	private void applyMetadata(final Metadata metadata, final String name, final MetadataValueConsumer consumer) throws
			IOException {
		String value = metadata.get(name);

		if (null == value || value.isEmpty()) {
			return;
		}

		consumer.accept(fields.forMetadata(name), value);

		// Add a separate field containing the ISO 8601 date.
		if (isoDates && dateFieldNames.contains(name)) {
			final String isoDate;

			try {
				isoDate = ensureISODate(value);
			} catch (DateTimeParseException e) {
				throw new SpewerException(String.format("Unable to parse date \"%s\" from field \"%s\" for " +
						"ISO 8601 formatting.", name, value), e);
			}

			consumer.accept(fields.forMetadataISODate(name), isoDate);
		}
	}

	private String ensureISODate(final String value) throws DateTimeParseException {
		Instant instant = null;

		for (DateTimeFormatter format: dateFormats) {
			try {
				instant = format.parse(value, Instant::from);
			} catch (DateTimeParseException e) {
				continue;
			}

			break;
		}

		if (null == instant) {
			throw new DateTimeParseException("Unable to parse date to ISO 8601 format.", value, 0);
		}

		return instant.toString();
	}

	private static final List<String> dateFieldNames = Arrays.asList(
			DublinCore.DATE.getName(),
			DublinCore.CREATED.getName(),
			DublinCore.MODIFIED.getName(),
			Office.CREATION_DATE.getName(),
			Office.SAVE_DATE.getName(),
			MSOffice.CREATION_DATE.getName(),
			MSOffice.LAST_SAVED.getName(),
			PDF.DOC_INFO_CREATED.getName(),
			PDF.DOC_INFO_MODIFICATION_DATE.getName(),
			Metadata.DATE.getName(),
			Metadata.MODIFIED,
			Metadata.LAST_MODIFIED.getName());

	private static final List<DateTimeFormatter> dateFormats = Arrays.asList(
			DateTimeFormatter.ISO_INSTANT,
			DateTimeFormatter.ISO_OFFSET_DATE_TIME,
			DateTimeFormatter.ISO_LOCAL_DATE_TIME,
			DateTimeFormatter.RFC_1123_DATE_TIME,
			DateTimeFormatter.ISO_DATE);

	@FunctionalInterface
	interface MetadataValueConsumer {

		void accept(final String name, final String value) throws IOException;
	}

	@FunctionalInterface
	interface MetadataValuesConsumer {

		void accept(final String name, final String[] values) throws IOException;
	}
}
