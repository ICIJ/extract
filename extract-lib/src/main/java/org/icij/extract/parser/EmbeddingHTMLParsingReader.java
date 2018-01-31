package org.icij.extract.parser;

import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.ContentHandlerDecorator;
import org.apache.tika.sax.ExpandedTitleContentHandler;
import org.icij.extract.document.EmbeddedTikaDocument;
import org.icij.extract.document.TikaDocument;
import org.icij.extract.io.DataURIEncodingInputStream;
import org.icij.extract.io.TokenReplacingReader;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

import java.io.IOException;

import static org.apache.tika.sax.XHTMLContentHandler.XHTML;

/**
 * Example:
 *
 * <code>
 *     			final String uuid = UUID.randomUUID().toString();
 *     			final String open = uuid + "/";
 *     			final String close = "/" + uuid;
 *     			context.set(Parser.class, EmptyParser.INSTANCE);
 *     			context.set(EmbeddedDocumentExtractor.class, new EmbedLinker(document, tmp, open, close));
 *     			reader = new EmbeddingHTMLParsingReader(document, open, close, parser, input, metadata, context);
 * </code>
 *
 * @since 1.0.0-beta
 */
public class EmbeddingHTMLParsingReader extends ParsingReader {

	private final TokenReplacingReader replacer;

	public EmbeddingHTMLParsingReader(final TikaDocument parent, final String open, final String close, final Parser
			parser, final TikaInputStream input, final Metadata metadata, final ParseContext context) throws
			IOException {
		super(parser, input, metadata, context, (writer)-> new SubstitutingContentHandler(parent, open, close, new
				ExpandedTitleContentHandler(new HTML5Serializer(writer))));
		this.replacer = new TokenReplacingReader((token)-> {
			final EmbeddedTikaDocument embed = parent.getEmbed(token);

			if (null == embed) {
				return null;
			}

			return DataURIEncodingInputStream.createReader(embed.getPath(), embed.getMetadata());
		}, reader, open, close);
	}

	@Override
	public int read(char[] buffer, int offset, int length) throws IOException {
		return replacer.read(buffer, offset, length);
	}

	@Override
	public void close() throws IOException {
		replacer.close(); // Closes the underlying reader.
		super.close();
	}

	private static class SubstitutingContentHandler extends ContentHandlerDecorator {

		private boolean isEmbeddedImgTagOpen = false;
		private boolean isEmbeddedAnchorTagOpen = false;
		private boolean anchorTagDropped = false;
		private AttributesImpl imgAttributes = null;

		private static final String IMG_TAG = "img";
		private static final String ANCHOR_TAG = "a";

		private final TikaDocument parent;
		private final String open;
		private final String close;

		SubstitutingContentHandler(final TikaDocument parent, final String open, final String close, final ContentHandler
				handler) {
			super(handler);
			this.open = open;
			this.close = close;
			this.parent = parent;
		}

		@Override
		public void startElement(final String uri, final String localName, final String qName, final Attributes atts)
				throws SAXException {
			final AttributesImpl attributes = new AttributesImpl(atts);

			if (IMG_TAG.equalsIgnoreCase(localName) && XHTML.equals(uri) && !isEmbeddedImgTagOpen &&
					!isEmbeddedAnchorTagOpen) {

				final String src = attributes.getValue("", "src");

				if (null != src && src.startsWith("embedded:")) {
					isEmbeddedImgTagOpen = true;
					imgAttributes = attributes;

				} else if (null != src && src.startsWith("cid:")) {
					super.startElement(uri, localName, qName, atts);
				}

			} else if (ANCHOR_TAG.equalsIgnoreCase(localName) && XHTML.equals(uri) && !isEmbeddedAnchorTagOpen) {

				final String href = attributes.getValue("", "href");
				String path = null;

				if (null != href && href.startsWith(open) && href.endsWith(close)) {
					path = href.substring(open.length(), href.length() - close.length());
				}

				if (null != path && null != parent.getEmbed(path)) {
					isEmbeddedAnchorTagOpen = true;

					// Drop the anchor tag if coming after an embedded image.
					if (isEmbeddedImgTagOpen) {
						imgAttributes.setAttribute(imgAttributes.getIndex("", "src"), "", "src", "src",
								"CDATA", href);
						super.startElement(uri, IMG_TAG, IMG_TAG, imgAttributes);
						super.endElement(uri, IMG_TAG, IMG_TAG);
						isEmbeddedImgTagOpen = false;
						imgAttributes = null;
						anchorTagDropped = true;
					} else {
						super.startElement(uri, localName, qName, attributes);
						anchorTagDropped = false;
					}
				} else if (isEmbeddedImgTagOpen) {
					isEmbeddedImgTagOpen = false;
					imgAttributes = null;
					super.startElement(uri, localName, qName, attributes);
				}
			} else {
				if (isEmbeddedAnchorTagOpen) {
					isEmbeddedAnchorTagOpen = false;
					anchorTagDropped = false;
				}

				if (isEmbeddedImgTagOpen) {
					isEmbeddedImgTagOpen = false;
					imgAttributes = null;
				}

				super.startElement(uri, localName, qName, attributes);
			}
		}

		@Override
		public void characters(char[] ch, int start, int length) throws SAXException {

			// Swallow the text in between UUID start and close tags.
			if (!isEmbeddedAnchorTagOpen || !anchorTagDropped) {
				super.characters(ch, start, length);
			}
		}

		@Override
		public void endElement(String uri, String localName, String qName) throws SAXException {

			// Swallow the event if closing an embedded image tag.
			if (isEmbeddedImgTagOpen && IMG_TAG.equalsIgnoreCase(localName) && XHTML.equals(uri)) {
				return;
			}

			// Error state. Output this event and the previous one.
			if (isEmbeddedImgTagOpen) {
				super.startElement(uri, IMG_TAG, IMG_TAG, imgAttributes);
				isEmbeddedImgTagOpen = false;
				imgAttributes = null;
			}

			if (isEmbeddedAnchorTagOpen && ANCHOR_TAG.equalsIgnoreCase(localName) && XHTML.equals(uri)) {
				isEmbeddedAnchorTagOpen = false;
			} else {
				super.endElement(uri, localName, qName);
			}
		}
	}
}
