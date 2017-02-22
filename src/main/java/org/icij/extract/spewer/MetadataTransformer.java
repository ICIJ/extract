package org.icij.extract.spewer;

import org.apache.commons.io.TaggedIOException;
import org.apache.tika.metadata.*;

import java.io.IOException;
import java.io.Serializable;

import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;
import java.util.List;
import java.util.Date;
import java.util.stream.Stream;

public class MetadataTransformer implements Serializable {

	private static final Map<String, Property> dateProperties = new HashMap<>();
	@SuppressWarnings("deprecation")
	private static final List<String> deduplicateProperties = Arrays.asList(Metadata.CONTENT_TYPE,
			TikaCoreProperties.TITLE.getName(),
			Metadata.TITLE);

	private static final long serialVersionUID = -6643888792096975746L;

	static {

		//noinspection deprecation
		Stream.of(
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
				Property.externalDate(Metadata.MODIFIED),
				HttpHeaders.LAST_MODIFIED).forEach(property -> dateProperties.put(property.getName(), property));
	}

	private final Metadata metadata;
	private final FieldNames fields;

	MetadataTransformer(final Metadata metadata, final FieldNames fields) {
		this.metadata = metadata;
		this.fields = fields;
	}

	void transform(final ValueConsumer single, final ValueArrayConsumer multiple) throws IOException {
		try {
			for (String name : metadata.names()) {
				if (metadata.isMultiValued(name)) {
					transform(name, multiple);
				} else {
					transform(name, single);
				}
			}
		} catch (IOException e) {
			throw new TaggedIOException(e, getClass());
		}
	}

	private void transform(final String name, final ValueArrayConsumer consumer) throws IOException {
		String[] values = metadata.getValues(name);
		Stream<String> stream = Arrays.stream(values);

		// Remove empty values.
		stream = stream.filter(value -> null != value && !value.isEmpty());

		// Remove duplicate:
		// 1) content types (Tika seems to add these sometimes, especially for RTF files);
		// 2) titles (appear in bad HTML files).
		if (values.length > 1 && deduplicateProperties.contains(name)) {
			stream = stream.distinct();
		}

		values = stream.toArray(String[]::new);
		if (values.length > 0) {
			consumer.accept(fields.forMetadata(name), values);
		}
	}

	private void transform(final String name, final ValueConsumer consumer) throws IOException {
		final String value = metadata.get(name);

		if (null == value || value.isEmpty()) {
			return;
		}

		consumer.accept(fields.forMetadata(name), value);

		// Add a separate field containing the ISO 8601 date.
		if (dateProperties.containsKey(name)) {
			transformDate(name, consumer);
		}
	}

	private void transformDate(final String name, final ValueConsumer consumer) throws IOException {
		final Date isoDate = metadata.getDate(dateProperties.get(name));

		if (null != isoDate) {
			consumer.accept(fields.forMetadataISODate(name), isoDate.toInstant().toString());
		} else {
			throw new IOException(String.format("Unable to parse date \"%s\" from field " +
					"\"%s\" for ISO 8601 formatting.", metadata.get(name), name));
		}
	}

	@FunctionalInterface
	interface ValueConsumer {

		void accept(final String name, final String value) throws IOException;
	}

	@FunctionalInterface
	interface ValueArrayConsumer {

		void accept(final String name, final String[] values) throws IOException;
	}
}
