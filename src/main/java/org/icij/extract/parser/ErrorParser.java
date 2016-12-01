package org.icij.extract.parser;

import java.util.Set;

import java.io.InputStream;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.mime.MediaType;
import org.apache.tika.exception.TikaException;

import org.xml.sax.ContentHandler;

/**
 * An error parser that should be used as a fallback for the default composite parser.
 *
 * This parser throws a {@link TikaException} for files that could not be parsed because they are not supported by
 * any known parsers and an {@link ExcludedMediaTypeException} for files that could not be parsed because the parser
 * that would otherwise handle them was excluded.
 *
 * @since 1.0.0-beta
 */
public class ErrorParser implements Parser {

	private static final long serialVersionUID = -4224235288173500115L;
	private final Parser parser;
	private final Set<MediaType> excludedTypes;

	public ErrorParser(final Parser parser, final Set<MediaType> excludedTypes) {
		this.parser = parser;
		this.excludedTypes = excludedTypes;
	}

	@Override
	public Set<MediaType> getSupportedTypes(final ParseContext context) {
		return parser.getSupportedTypes(context);
	}

	@Override
	public void parse(final InputStream inputStream, final ContentHandler contentHandler, final Metadata metadata,
	                  final ParseContext context) throws TikaException {
		final MediaType unsupportedType = MediaType.parse(metadata.get(Metadata.CONTENT_TYPE));

		if (null == unsupportedType) {
			throw new TikaException("Unable to parse unsupported media type: " + metadata.get(Metadata.CONTENT_TYPE) + ".");
		}

		// If the MIME type is supported by any of the excluded parsers, send a special exception to signal the reason.
		if (excludedTypes.contains(unsupportedType)) {
			throw new ExcludedMediaTypeException("Excluded media type: " + unsupportedType);
		}

		throw new TikaException("Unsupported media type: " + unsupportedType + ".");
	}
}
