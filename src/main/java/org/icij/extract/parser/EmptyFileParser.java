package org.icij.extract.parser;

import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.*;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Set;

public class EmptyFileParser extends AbstractParser {

	public static Parser INSTANCE = new EmptyFileParser();

	private static final long serialVersionUID = 2962493551622366449L;

	@Override
	public Set<MediaType> getSupportedTypes(final ParseContext context) {
		return Collections.emptySet();
	}

	@Override
	public void parse(final InputStream stream, final ContentHandler handler, final Metadata metadata,
	                  final ParseContext context) throws SAXException, IOException, TikaException {
		final Parser parser;
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

		// If the file is not empty, throw a parse error.
		// Otherwise, output an empty document.
		if (size > 0) {
			parser = ErrorParser.INSTANCE;
		} else {
			metadata.set(Metadata.CONTENT_TYPE, "application/octet-stream");
			parser = EmptyParser.INSTANCE;
		}

		parser.parse(stream, handler, metadata, context);
	}
}
