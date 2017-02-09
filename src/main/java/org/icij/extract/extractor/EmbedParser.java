package org.icij.extract.extractor;

import java.io.*;

import org.apache.tika.extractor.ParsingEmbeddedDocumentExtractor;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.DelegatingParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.io.TemporaryResources;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.io.CloseShieldInputStream;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.EmbeddedContentHandler;
import org.apache.tika.exception.TikaException;
import org.apache.tika.exception.EncryptedDocumentException;

import org.icij.extract.document.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.xml.sax.ContentHandler;
import org.xml.sax.helpers.AttributesImpl;
import org.xml.sax.SAXException;

import static org.apache.tika.sax.XHTMLContentHandler.XHTML;

/**
 * A custom extractor that is an almost exact copy of Tika's default extractor for embedded documents.
 *
 * Logs errors that Tika's default extractor otherwise swallows, but doesn't throw them, allowing parsing to continue.
 *
 * @since 1.0.0-beta
 */
public class EmbedParser extends ParsingEmbeddedDocumentExtractor {

	private static final Logger logger = LoggerFactory.getLogger(EmbedParser.class);
    private static final Parser DELEGATING_PARSER = new DelegatingParser();

	final Document root;
	protected final ParseContext context;

	EmbedParser(final Document root, final ParseContext context) {
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

	void delegateParsing(final InputStream input, final ContentHandler handler, final Metadata metadata)
			throws IOException, SAXException {
		try (final TemporaryResources tmp = new TemporaryResources()) {
			final TikaInputStream newStream = TikaInputStream.get(new CloseShieldInputStream(input), tmp);
			if (input instanceof TikaInputStream) {
				final Object container = ((TikaInputStream) input).getOpenContainer();
				if (container != null) {
					newStream.setOpenContainer(container);
				}
			}

			// Use the delegate parser to parse this entry.
			DELEGATING_PARSER.parse(newStream, handler, metadata, context);
		} catch (EncryptedDocumentException e) {
			logger.error("Encrypted document embedded in document: \"{}\" (in \"{}\").",
					metadata.get(Metadata.RESOURCE_NAME_KEY), root, e);
		} catch (TikaException e) {
			logger.error("Unable to parse embedded document: \"{}\" (in \"{}\").",
					metadata.get(Metadata.RESOURCE_NAME_KEY), root, e);
		}
	}

	void writeStart(final ContentHandler handler, final Metadata metadata) throws SAXException {
		final AttributesImpl attributes = new AttributesImpl();
		final String name = metadata.get(Metadata.RESOURCE_NAME_KEY);

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
