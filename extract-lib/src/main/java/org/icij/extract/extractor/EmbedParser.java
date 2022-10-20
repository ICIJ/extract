package org.icij.extract.extractor;

import org.apache.commons.io.input.CloseShieldInputStream;
import org.apache.tika.exception.EncryptedDocumentException;
import org.apache.tika.exception.TikaException;
import org.apache.tika.extractor.ParsingEmbeddedDocumentExtractor;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.parser.DelegatingParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.EmbeddedContentHandler;
import org.apache.tika.utils.ExceptionUtils;
import org.icij.extract.document.TikaDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

import java.io.IOException;
import java.io.InputStream;

import static org.apache.tika.sax.XHTMLContentHandler.XHTML;

/**
 * A custom extractor that is an almost exact copy of Tika's default extractor for embedded documents.
 *
 * Logs errors that Tika's default extractor otherwise swallows, but doesn't throw them, allowing parsing to continue.
 *
 * @since 1.0.0-beta
 */
public class EmbedParser extends ParsingEmbeddedDocumentExtractor {

	static final Logger logger = LoggerFactory.getLogger(EmbedParser.class);
    private static final Parser DELEGATING_PARSER = new DelegatingParser();

	final TikaDocument root;
	protected final ParseContext context;

	EmbedParser(final TikaDocument root, final ParseContext context) {
		super(context);
		this.root = root;
		this.context = context;
	}

	@Override
	public void parseEmbedded(final InputStream input, final ContentHandler handler, final Metadata metadata,
	                          final boolean outputHtml) throws SAXException, IOException {
		if (outputHtml) {
			writeStart(handler, metadata);
		}

		delegateParsing(input, new EmbeddedContentHandler(new BodyContentHandler(handler)), metadata);

		if (outputHtml) {
			writeEnd(handler);
		}
	}

	void delegateParsing(final InputStream input, final ContentHandler handler, final Metadata metadata) throws IOException, SAXException {
		try (final TikaInputStream tis = TikaInputStream.get(CloseShieldInputStream.wrap(input))) {
			if (input instanceof TikaInputStream) {
				final Object container = ((TikaInputStream) input).getOpenContainer();

				if (container != null) {
					tis.setOpenContainer(container);
				}
			}

			// Use the delegate parser to parse this entry.
			DELEGATING_PARSER.parse(tis, handler, metadata, context);
		} catch (final EncryptedDocumentException e) {
			logger.error("Unable to decrypt encrypted document embedded in document: \"{}\" ({}) (in \"{}\").",
					metadata.get(TikaCoreProperties.RESOURCE_NAME_KEY), metadata.get(Metadata.CONTENT_TYPE), root, e);
			metadata.add(TikaCoreProperties.TIKA_META_EXCEPTION_EMBEDDED_STREAM,
					ExceptionUtils.getFilteredStackTrace(e));
		} catch (final TikaException e) {
			logger.error("Unable to parse embedded document: \"{}\" ({}) (in \"{}\").",
					metadata.get(TikaCoreProperties.RESOURCE_NAME_KEY), metadata.get(Metadata.CONTENT_TYPE), root, e);
			metadata.add(TikaCoreProperties.TIKA_META_EXCEPTION_EMBEDDED_STREAM,
					ExceptionUtils.getFilteredStackTrace(e));
		}
	}

	void writeStart(final ContentHandler handler, final Metadata metadata) throws SAXException {
		final AttributesImpl attributes = new AttributesImpl();
		final String name = metadata.get(TikaCoreProperties.RESOURCE_NAME_KEY);

		attributes.addAttribute("", "class", "class", "CDATA", "package-entry");
		handler.startElement(XHTML, "div", "div", attributes);

		if (name != null && name.length() > 0) {
			handler.startElement(XHTML, "h1", "h1", new AttributesImpl());
			char[] chars = name.toCharArray();
			handler.characters(chars, 0, chars.length);
			handler.endElement(XHTML, "h1", "h1");
		}
	}

	void writeEnd(final ContentHandler handler) throws SAXException {
		handler.endElement(XHTML, "div", "div");
	}
}
