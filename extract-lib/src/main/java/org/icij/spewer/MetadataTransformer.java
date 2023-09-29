package org.icij.spewer;

import org.apache.commons.io.TaggedIOException;
import org.apache.tika.metadata.*;

import java.io.IOException;
import java.io.Serializable;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;

import java.util.*;
import java.util.stream.Stream;

public class MetadataTransformer implements Serializable {

	private static final List<DateTimeFormatter> dateFormats = new ArrayList<>();
	private static final Map<String, Property> dateProperties = new HashMap<>();
	@SuppressWarnings("deprecation")
	private static final List<String> deduplicateProperties;
	private static final String TITLE = "title";

	static {
		deduplicateProperties = Arrays.asList(

			// Deduplicate content types (Tika seems to add these sometimes, especially for RTF files).
			Metadata.CONTENT_TYPE.toLowerCase(Locale.ENGLISH),

				// Deduplicate titles (appear in bad HTML files).
				TikaCoreProperties.TITLE.getName().toLowerCase(Locale.ENGLISH),

				// Deduplicate these properties contained in some MSHTML documents.
				TITLE,
				"originator",
				"generator",
				"progid");
	}

	private static final long serialVersionUID = -6643888792096975746L;

	static {
		dateFormats.add(DateTimeFormatter.RFC_1123_DATE_TIME);
		dateFormats.add(DateTimeFormatter.ofPattern("EEE MMM d HH:mm:ss uuuu", Locale.ENGLISH)); // Example: "Tue Jan 27 17:03:21 2004"

		//noinspection deprecation
		Stream.of(
				DublinCore.DATE,
				DublinCore.CREATED,
				DublinCore.MODIFIED,
				Office.CREATION_DATE,
				Office.SAVE_DATE,
				Office.PRINT_DATE,
				PDF.DOC_INFO_CREATED,
				PDF.DOC_INFO_MODIFICATION_DATE,
				TIFF.ORIGINAL_DATE,
				Property.externalDate(TikaCoreProperties.MODIFIED.getName()))
				.forEach(property -> dateProperties.put(property.getName(), property));
	}

	private final Metadata metadata;
	private final FieldNames fields;
	private final Map<String, String> fieldMap = new HashMap<>();
	private final MetadataBlockList metadata_block_list = new MetadataBlockList();

	public MetadataTransformer(final Metadata metadata, final FieldNames fields) {
		this.metadata = metadata;
		this.fields = fields;
	}

	public void transform(final ValueConsumer single, final ValueArrayConsumer multiple) throws IOException {
		final Map<String, String[]> normalised = new HashMap<>();

		// Loop over the names twice, first to normalise the names so that "GENERATOR" and "Generator" get normalised
		// to "generator", and the values are concatenated into an array instead of one value overriding the other in
		// the consumer.
		for (String name : metadata.names()) {
			String[] values = metadata.getValues(name);

			if (0 == values.length) {
				continue;
			}

			// The title field should not be considered multivalued until TIKA-2274 is resolved.
			//noinspection deprecation
			if (values.length > 1 && name.equals(TITLE)) {
				values = Arrays.copyOfRange(values, 0, 1);
			}

			// Keep a mapping of the old name around, to enable a reverse lookup later.
			final String normalisedName = fields.forMetadata(name);

			// The field name might be blocked
			if (!metadata_block_list.ok(normalisedName)) {
				continue;
			}

			fieldMap.putIfAbsent(normalisedName, name);
			normalised.merge(normalisedName, values, this::concat);
		}

		try {
			for (Map.Entry<String, String[]> entry: normalised.entrySet()) {
				final String[] values = entry.getValue();

				if (values.length > 1) {
					transform(entry.getKey(), values, multiple);
				} else {
					transform(entry.getKey(), values[0], single);
				}
			}
		} catch (IOException e) {
			throw new TaggedIOException(e, getClass());
		}
	}

	private String[] concat(final String[] a, final String[] b) {
		final String[] n;

		n = new String[a.length + b.length];

		System.arraycopy(a, 0, n, 0, a.length);
		System.arraycopy(b, 0, n, a.length, b.length);

		return n;
	}

	private void transform(final String normalisedName, String[] values, final ValueArrayConsumer consumer) throws
			IOException {
		Stream<String> stream = Arrays.stream(values);

		// Remove empty values.
		stream = stream.filter(value -> null != value && !value.isEmpty());

		// Remove duplicates.
		// Normalised to lowercase so that "GENERATOR" matches "Generator" (these inconsistent names can come from
		// HTML documents).
		if (values.length > 1 && deduplicateProperties.contains(fieldMap.get(normalisedName)
				.toLowerCase(Locale.ENGLISH))) {
			stream = stream.distinct();
		}

		values = stream.toArray(String[]::new);
		if (values.length > 0) {
			consumer.accept(normalisedName, values);
		}
	}

	private void transform(final String normalisedName, final String value, final ValueConsumer consumer) throws
			IOException {
		if (null == value || value.isEmpty()) {
			return;
		}

		consumer.accept(normalisedName, value);

		// Add a separate field containing the ISO 8601 date.
		final String name = fieldMap.get(normalisedName);

		if (dateProperties.containsKey(name)) {
			transformDate(name, consumer);
		}
	}

	private void transformDate(final String name, final ValueConsumer consumer) throws IOException {
		final Date date = metadata.getDate(dateProperties.get(name));
		Instant instant = null;

		if (null != date) {
			instant = date.toInstant();
		} else {

			// Try some other formats.
			for (DateTimeFormatter format: dateFormats) {
				final TemporalAccessor accessor;

				try {
					accessor = format.parseBest(metadata.get(name), Instant::from, LocalDateTime::from);
				} catch (final DateTimeParseException e) {
					continue;
				}

				if (accessor instanceof Instant) {
					instant = (Instant) accessor;
				} else if (accessor instanceof LocalDateTime) {

					// Default to UTC for dates with not time zone.
					instant = ((LocalDateTime) accessor).toInstant(ZoneOffset.UTC);
				}

				break;
			}
		}

		if (null != instant) {
			consumer.accept(fields.forMetadataISODate(name), instant.toString());
		} else {
			throw new IOException(String.format("Unable to parse date \"%s\" from field " +
					"\"%s\" for ISO 8601 formatting.", metadata.get(name), name));
		}
	}

	@FunctionalInterface
	public interface ValueConsumer {
		void accept(final String name, final String value) throws IOException;
	}

	@FunctionalInterface
	public interface ValueArrayConsumer {
		void accept(final String name, final String[] values) throws IOException;
	}
}
