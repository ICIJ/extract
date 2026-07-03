package org.icij.spewer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.TaggedIOException;
import org.apache.tika.metadata.DublinCore;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.Office;
import org.apache.tika.metadata.PDF;
import org.apache.tika.metadata.Property;
import org.apache.tika.metadata.TIFF;
import org.apache.tika.metadata.TikaCoreProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

public class MetadataTransformer implements Serializable {

	private static final Logger logger = LoggerFactory.getLogger(MetadataTransformer.class);
	private static final List<DateTimeFormatter> dateFormats = new ArrayList<>();
	private static final Map<String, Property> dateProperties = new HashMap<>();
	@SuppressWarnings("deprecation")
	private static final List<String> deduplicateProperties;
	private static final String TITLE = "title";
	private static final ObjectMapper MAPPER = new ObjectMapper();

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

	public MetadataTransformer(Metadata metadata) {
		this.metadata = metadata;
		this.fields = new FieldNames();
	}

	public static Metadata loadMetadata(File file) throws IOException {
		return loadMetadata(file, false);
	}

	public static Metadata loadMetadata(File file, boolean denormalizeKeys) throws IOException {
		Map<String, String[]> normalizedMap = MAPPER.readValue(Files.readString(file.toPath()), new TypeReference<>() {});
		Metadata metadata = new Metadata();
		normalizedMap.forEach((k, v) -> Arrays.stream(v).forEach(s -> metadata.add(denormalizeKeys ? k.replace("tika_metadata_", ""):k, s)));
		return metadata;
	}

	public void transform(final ValueConsumer single, final ValueArrayConsumer multiple) throws IOException {
		final Map<String, String[]> normalised = normalize(metadata);

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

	private Map<String, String[]> normalize(Metadata metadata) {
		return normalize(metadata, true);
	}

	private Map<String, String[]> normalize(Metadata metadata, boolean normalizeKeys) {
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
			final String normalisedName = normalizeKeys ? fields.forMetadata(name): name;

			// The field name might be blocked
			if (!metadata_block_list.ok(normalisedName)) {
				continue;
			}

			fieldMap.putIfAbsent(normalisedName, name);
			normalised.merge(normalisedName, values, this::concat);
		}
		return normalised;
	}

	public String transform() throws JsonProcessingException {
		return MAPPER.writeValueAsString(normalize(metadata, false));
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
		final Optional<Instant> instant = (null != date)
				? Optional.of(date.toInstant())
				: parseFallbackDate(metadata.get(name));

		if (instant.isPresent()) {
			consumer.accept(fields.forMetadataISODate(name), instant.get().toString());
		} else {

			// Degrade instead of failing: the raw value has already been emitted by the caller, so a
			// date we cannot normalize must never veto the whole document. Skip only the ISO variant.
			logger.warn("Unable to parse date \"{}\" from field \"{}\" for ISO 8601 formatting; " +
					"keeping raw value.", metadata.get(name), name);
		}
	}

	/**
	 * Parse a single, possibly lenient date value into an {@link Instant} for the ISO-8601 variant.
	 * Tries the {@link #dateFormats} fallbacks (RFC-1123 and C asctime) after collapsing runs of
	 * whitespace to a single space, so double-space-padded asctime days (e.g. "Thu May  1 …") match.
	 * Also accepts bare epoch seconds and epoch milliseconds. Returns empty when nothing matches so
	 * the caller can degrade gracefully rather than fail the document.
	 */
	static Optional<Instant> parseFallbackDate(final String value) {
		if (null == value || value.isBlank()) {
			return Optional.empty();
		}

		final String normalised = value.trim().replaceAll("\\s+", " ");

		for (final DateTimeFormatter format: dateFormats) {
			final TemporalAccessor accessor;

			try {
				accessor = format.parseBest(normalised, Instant::from, LocalDateTime::from);
			} catch (final DateTimeParseException e) {
				continue;
			}

			if (accessor instanceof Instant) {
				return Optional.of((Instant) accessor);
			} else if (accessor instanceof LocalDateTime) {

				// Default to UTC for dates with no time zone.
				return Optional.of(((LocalDateTime) accessor).toInstant(ZoneOffset.UTC));
			}
		}

		return parseEpoch(normalised);
	}

	private static Optional<Instant> parseEpoch(final String value) {
		if (value.isEmpty() || !value.chars().allMatch(Character::isDigit)) {
			return Optional.empty();
		}

		try {
			final long epoch = Long.parseLong(value);

			// Heuristic on digit length: 13 or more digits are epoch milliseconds, 10 to 12 digits are
			// epoch seconds. Shorter all-digit values (e.g. a bare "yyyyMMdd") are not treated as epochs.
			if (value.length() >= 13) {
				return Optional.of(Instant.ofEpochMilli(epoch));
			}
			if (value.length() >= 10) {
				return Optional.of(Instant.ofEpochSecond(epoch));
			}
			return Optional.empty();
		} catch (final NumberFormatException e) {
			return Optional.empty();
		}
	}

	/**
	 * Parse a single metadata value into an {@link Instant}, accepting only the ISO-8601 forms
	 * Elasticsearch's date_detection recognizes (strict_date_optional_time). This keeps the set of
	 * fields we reshape into date arrays equal to the set ES would map as `date`. Lenient formats
	 * (e.g. RFC-1123) are deliberately not matched: ES maps such fields as text/keyword, so they are
	 * safely left to comma-join.
	 */
	static Optional<Instant> parseInstant(final String value) {
		if (null == value || value.isEmpty()) {
			return Optional.empty();
		}

		try {
			return Optional.of(Instant.parse(value));
		} catch (final DateTimeParseException ignored) {
			// not a Z/offset instant; try the next form
		}

		try {
			return Optional.of(OffsetDateTime.parse(value).toInstant());
		} catch (final DateTimeParseException ignored) {
			// not an offset date-time; try the next form
		}

		try {
			return Optional.of(LocalDateTime.parse(value).toInstant(ZoneOffset.UTC));
		} catch (final DateTimeParseException ignored) {
			// not a zoneless date-time; try the next form
		}

		try {
			return Optional.of(LocalDate.parse(value).atStartOfDay(ZoneOffset.UTC).toInstant());
		} catch (final DateTimeParseException ignored) {
			// not a bare date
		}

		return Optional.empty();
	}

	/**
	 * Normalize a multi-valued field to ISO-8601 instants. Returns the list of normalized values
	 * (order preserved) only if every value parses as a date, so that a mixed field falls back to
	 * the caller's default handling instead of emitting a partially-valid date array.
	 */
	static Optional<List<String>> toIso8601Array(final String[] values) {
		final List<String> normalised = new ArrayList<>(values.length);

		for (final String value : values) {
			final Optional<Instant> instant = parseInstant(value);
			if (instant.isEmpty()) {
				return Optional.empty();
			}
			normalised.add(instant.get().toString());
		}

		return normalised.isEmpty() ? Optional.empty() : Optional.of(normalised);
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
