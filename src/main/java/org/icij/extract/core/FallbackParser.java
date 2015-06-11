package org.icij.extract.core;

import java.util.Set;
import java.util.List;

import java.io.InputStream;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.mime.MediaType;
import org.apache.tika.exception.TikaException;

import org.xml.sax.ContentHandler;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public class FallbackParser implements Parser {

	private final Parser parser;
	private final List<Parser> excludedParsers;

	public FallbackParser(Parser parser, List<Parser> excludedParsers) {
		this.parser = parser;
		this.excludedParsers = excludedParsers;
	}

	@Override
	public Set<MediaType> getSupportedTypes(ParseContext context) {
		return parser.getSupportedTypes(context);
	}

	@Override
	public void parse(InputStream inputStream, ContentHandler contentHandler, Metadata metadata, ParseContext context) throws TikaException {
		final MediaType unsupportedType = MediaType.parse(metadata.get(Metadata.CONTENT_TYPE));

		if (null == unsupportedType) {
			throw new TikaException("Unable to parse unsupported media type: " + metadata.get(Metadata.CONTENT_TYPE) + ".");
		}

		// If the MIME type is supported by any of the excluded parsers, send a special exception to signal the reason.
		if (excludedParsersSupportType(unsupportedType, context)) {
			throw new ExcludedMediaTypeException("Excluded media type: " + unsupportedType);
		} else {
			throw new TikaException("Unsupported media type: " + unsupportedType + ".");
		}
	}

	private boolean excludedParsersSupportType(final MediaType type, final ParseContext context) {
		for (Parser parser : excludedParsers) {
			if (parserSupportsType(type, parser, context)) {
				return true;
			}
		}

		return false;
	}

	private boolean parserSupportsType(final MediaType type, final Parser parser, final ParseContext context) {
		for (MediaType supportedType : parser.getSupportedTypes(context)) {
			if (supportedType.equals(type)) {
				return true;
			}
		}

		return false;
	}
}
