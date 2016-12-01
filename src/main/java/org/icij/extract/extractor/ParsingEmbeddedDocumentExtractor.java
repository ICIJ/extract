package org.icij.extract.core;

import java.io.*;

import java.nio.file.Path;

import org.apache.tika.io.IOUtils;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.DelegatingParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.extractor.DocumentSelector;
import org.apache.tika.extractor.EmbeddedDocumentExtractor;
import org.apache.tika.io.TemporaryResources;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.io.CloseShieldInputStream;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.EmbeddedContentHandler;
import org.apache.tika.exception.TikaException;
import org.apache.tika.exception.EncryptedDocumentException;

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
class ParsingEmbeddedDocumentExtractor implements EmbeddedDocumentExtractor {

	private static final Logger logger = LoggerFactory.getLogger(ParsingEmbeddedDocumentExtractor.class);

    private static final File ABSTRACT_PATH = new File("");
    private static final Parser DELEGATING_PARSER = new DelegatingParser();

	private final Path parent;
	private final ParseContext context;

	ParsingEmbeddedDocumentExtractor(final Path parent, final ParseContext context) {
		this.parent = parent;
		this.context = context;
	}

	@Override
	public boolean shouldParseEmbedded(Metadata metadata) {
		final DocumentSelector selector = context.get(DocumentSelector.class);
		if (selector != null) {
			return selector.select(metadata);
		}

		final FilenameFilter filter = context.get(FilenameFilter.class);
		if (filter != null) {
			final String name = metadata.get(Metadata.RESOURCE_NAME_KEY);
			if (name != null) {
				return filter.accept(ABSTRACT_PATH, name);
			}
		}

		return true;
	}

	@Override
	public void parseEmbedded(InputStream input, ContentHandler handler, Metadata metadata, boolean outputHtml)
		throws SAXException, IOException {
		if (outputHtml) {
			final AttributesImpl attributes = new AttributesImpl();
			attributes.addAttribute("", "class", "class", "CDATA", "package-entry");
			handler.startElement(XHTML, "div", "div", attributes);
		}

		final String name = metadata.get(Metadata.RESOURCE_NAME_KEY);
		if (name != null && name.length() > 0 && outputHtml) {
			handler.startElement(XHTML, "h1", "h1", new AttributesImpl());
			char[] chars = name.toCharArray();
			handler.characters(chars, 0, chars.length);
			handler.endElement(XHTML, "h1", "h1");
		}

		// Use the delegate parser to parse this entry.
		try (final TemporaryResources tmp = new TemporaryResources()) {
			final TikaInputStream newStream = TikaInputStream.get(new CloseShieldInputStream(input), tmp);
			if (input instanceof TikaInputStream) {
				final Object container = ((TikaInputStream) input).getOpenContainer();
				if (container != null) {
					newStream.setOpenContainer(container);
				}
			}

			DELEGATING_PARSER.parse(newStream, new EmbeddedContentHandler(new BodyContentHandler(handler)), metadata, context);
		} catch (EncryptedDocumentException e) {
			logger.error(String.format("Encrypted document embedded in document: \"%s\".", parent), e);
		} catch (TikaException e) {
			logger.error(String.format("Unable to parse embedded document in document: \"%s\".", parent), e);
		}

		if (outputHtml) {
			handler.endElement(XHTML, "div", "div");
		}
	}
}
