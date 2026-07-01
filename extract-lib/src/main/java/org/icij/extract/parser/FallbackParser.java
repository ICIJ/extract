package org.icij.extract.parser;

import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.*;
import org.icij.extract.extractor.NoContentReason;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Set;

public class FallbackParser extends AbstractParser {

	public static Parser INSTANCE = new FallbackParser();

	private static final long serialVersionUID = 2962493551622366449L;

	@Override
	public Set<MediaType> getSupportedTypes(final ParseContext context) {
		return Collections.emptySet();
	}

	@Override
	public void parse(final InputStream stream, final ContentHandler handler, final Metadata metadata,
	                  final ParseContext context) throws SAXException, IOException, TikaException {
		final long size;
		String value = metadata.get(Metadata.CONTENT_LENGTH);

		if (null != value && !value.isEmpty()) {
			size = Long.valueOf(value);
		} else {
			try (final TikaInputStream tis = TikaInputStream.get(stream)) {
				size = tis.getLength();
			}

			metadata.set(Metadata.CONTENT_LENGTH, Long.toString(size));
		}

		// Nothing to extract. Index the document as empty and record why, instead of throwing.
		if (size > 0) {
			NoContentReason.stamp(metadata, NoContentReason.UNSUPPORTED_MEDIA_TYPE);
		} else {
			metadata.set(Metadata.CONTENT_TYPE, "application/octet-stream");
			NoContentReason.stamp(metadata, NoContentReason.EMPTY_FILE);
		}

		EmptyParser.INSTANCE.parse(stream, handler, metadata, context);
	}
}
