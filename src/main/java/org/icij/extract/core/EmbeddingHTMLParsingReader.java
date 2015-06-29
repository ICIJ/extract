package org.icij.extract.core;

import org.icij.extract.encoder.DataURIEncodingInputStream;

import java.util.UUID;
import java.util.Map;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import java.io.Reader;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import org.apache.tika.parser.Parser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.EmptyParser;
import org.apache.tika.extractor.EmbeddedDocumentExtractor;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.io.TemporaryResources;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.sax.ExpandedTitleContentHandler;
import org.apache.tika.sax.ContentHandlerDecorator;

import org.xml.sax.Attributes;
import org.xml.sax.helpers.AttributesImpl;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import org.icij.extract.replacer.TokenResolver;
import org.icij.extract.replacer.TokenReplacingReader;

import static org.apache.tika.sax.XHTMLContentHandler.XHTML;

/**
 * @since 1.0.0-beta
 */
public class EmbeddingHTMLParsingReader extends HTMLParsingReader {

	private final TokenReplacingReader replacer;
	private final TemporaryResources tmp = new TemporaryResources();
	private final Map<String, String> cidMap = new HashMap<String, String>();
	private final Map<String, Path> pathMap = new HashMap<String, Path>();
	private final Map<String, Metadata> metaMap = new HashMap<String, Metadata>();

	public EmbeddingHTMLParsingReader(Logger logger, Parser parser, TikaInputStream input,
		Metadata metadata, ParseContext context) throws IOException {

		super(logger, parser, input, metadata, context);
		replacer = new TokenReplacingReader(new UUIDTokenResolver(), reader, "uuid:{", "}");
	}

	@Override
	public int read(char[] cbuf, int off, int len) throws IOException {
		return replacer.read(cbuf, off, len);
	}

	@Override
	public void close() throws IOException {
		IOException thrown = null;

		try {
			tmp.close();
		} catch (IOException e) {
			thrown = e;
		}

		try {

			// Closes the underlying reader.
			replacer.close();
		} catch (IOException e) {
			thrown = e;
		}

		if (null != thrown) {
			throw thrown;
		}
	}

	@Override
	protected void execute() {
		executor.execute(new ParsingTask());
	}

	/**
	 * The background parsing task.
	 */
	protected class ParsingTask extends HTMLParsingReader.ParsingTask {

		@Override
		protected ContentHandler createHandler() {
			return new UUIDSubstitutingContentHandler(super.createHandler());
		}

		@Override
		public void run() {
			final Path parent;

			try {
				parent = ((TikaInputStream) input).getFile().toPath();
			} catch (IOException e) {
				throwable = e;
				return;
			}

			final EmbeddedDocumentExtractor extractor = new SavingEmbeddedDocumentExtractor(parent);

			context.set(Parser.class, EmptyParser.INSTANCE);
			context.set(EmbeddedDocumentExtractor.class, extractor);

			super.run();
		}
	}

	/**
	 * A custom extractor that saves all embeds to temporary files and records the new paths.
	 *
	 * Ideally {@link #parseEmbedded} would use {@link TikaCoreProperties.EMBEDDED_RESOURCE_TYPE}
	 * but only the PDF parser seems to support it as of Tika 1.8.
	 *
	 * @since 1.0.0-beta
	 */
	public class SavingEmbeddedDocumentExtractor implements EmbeddedDocumentExtractor {

		private final Path parent;
		private int untitled = 0;

		public SavingEmbeddedDocumentExtractor(final Path parent) {
			this.parent = parent;
		}

		public boolean shouldParseEmbedded(Metadata metadata) {

			// Files are not actually parsed. They are always embedded.
			return true;
		}

		public void parseEmbedded(InputStream input, ContentHandler handler, Metadata metadata,
			boolean outputHtml) throws SAXException, IOException {

			// Get the name of the embedded file or set to a default if null or empty.
			String name = metadata.get(Metadata.RESOURCE_NAME_KEY);
			if (null == name || name.isEmpty()) {
				name = String.format("untitled file %d", ++untitled);
			}

			final Path child = saveEmbedded(name, input);
			final String uuid = UUID.randomUUID().toString();

			pathMap.put(uuid, child);
			metaMap.put(uuid, metadata);

			// If outputHtml is false then it means that the parser already outputted
			// markup for the embed.
			if (outputHtml) {
				final AttributesImpl attributes = new AttributesImpl();
				final char[] chars = name.toCharArray();

				attributes.addAttribute("", "class", "class", "CDATA", "package-entry");
				handler.startElement(XHTML, "div", "div", attributes);
				handler.startElement(XHTML, "h1", "h1", new AttributesImpl());
				handler.characters(chars, 0, chars.length);
				handler.endElement(XHTML, "h1", "h1");
			}

			final AttributesImpl attributes = new AttributesImpl();
			final String type = metadata.get(Metadata.CONTENT_TYPE);

	        attributes.addAttribute("", "href", "href", "CDATA", "uuid:{" + uuid + "}");
	        attributes.addAttribute("", "title", "title", "CDATA", name);
			attributes.addAttribute("", "download", "download", "CDATA", name);

			if (null != type) {
				attributes.addAttribute("", "type", "type", "CDATA", type);
			}

	        handler.startElement(XHTML, "a", "a", attributes);
			char[] chars = name.toCharArray();
			handler.characters(chars, 0, chars.length);
	        handler.endElement(XHTML, "a", "a");

			if (outputHtml) {
				handler.endElement(XHTML, "div", "div");
			}
		}

		private Path saveEmbedded(final String name, final InputStream input) throws IOException {
			Path embed;

			try {
				embed = tmp.createTemporaryFile().toPath();
			} catch (IOException e) {
				logger.log(Level.SEVERE, String.format("Unable to create temporary file for embed in document: %s.", parent), e);
				throw e;
			}

			try {
				Files.copy(input, embed, StandardCopyOption.REPLACE_EXISTING);
			} catch (IOException e) {
				logger.log(Level.SEVERE, String.format("Unable to save embedded document \"%s\" in document: %s.", name, parent), e);
				throw e;
			} finally {
				input.close();
			}

			return embed;
		}
	}

	protected class UUIDSubstitutingContentHandler extends ContentHandlerDecorator {

		private boolean isEmbeddedImgTagOpen = false;
		private boolean isEmbeddedAnchorTagOpen = false;
		private AttributesImpl imgAtts = null;

		private static final String IMG_TAG = "img";
		private static final String ANCHOR_TAG = "a";

		public UUIDSubstitutingContentHandler(ContentHandler handler) {
			super(handler);
		}

		@Override
		public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
			final AttributesImpl attributes = new AttributesImpl(atts);

			if (IMG_TAG.equalsIgnoreCase(localName) && XHTML.equals(uri) &&
				!isEmbeddedImgTagOpen && !isEmbeddedAnchorTagOpen) {

				final String src = attributes.getValue("", "src");

				if (null != src && src.startsWith("embedded:")) {
					isEmbeddedImgTagOpen = true;
					imgAtts = attributes;

				} else if (null != src && src.startsWith("cid:")) {
					final String uuid = UUID.randomUUID().toString();

					cidMap.put(uuid, src.substring("cid:".length()));
					attributes.setAttribute(attributes.getIndex("", "src"), "", "src", "src", "CDATA", "uuid:{" + uuid + "}");
				}

			} else if (ANCHOR_TAG.equalsIgnoreCase(localName) && XHTML.equals(uri) &&
				!isEmbeddedAnchorTagOpen) {

				final String href = attributes.getValue("", "href");
				String uuid = null;

				if (null != href && href.startsWith("uuid:")) {
					uuid = href.substring("uuid:{".length(), href.length() - 1);
				}

				if (null != uuid && null != pathMap.get(uuid)) {
					isEmbeddedAnchorTagOpen = true;

					// Drop the anchor tag if coming after an embedded image.
					if (isEmbeddedImgTagOpen) {
						imgAtts.setAttribute(imgAtts.getIndex("", "src"), "", "src", "src", "CDATA", "uuid:{" + uuid + "}");
						super.startElement(uri, IMG_TAG, IMG_TAG, imgAtts);
						super.endElement(uri, IMG_TAG, IMG_TAG);
						isEmbeddedImgTagOpen = false;
						imgAtts = null;
					} else {
						super.startElement(uri, localName, qName, attributes);
						super.endElement(uri, localName, qName);
					}
				} else if (isEmbeddedImgTagOpen) {
					isEmbeddedImgTagOpen = false;
					imgAtts = null;
				}
			} else {
				if (isEmbeddedAnchorTagOpen) {
					isEmbeddedAnchorTagOpen = false;
				}

				if (isEmbeddedImgTagOpen) {
					isEmbeddedImgTagOpen = false;
					imgAtts = null;
				}
			}

			if (!isEmbeddedAnchorTagOpen && !isEmbeddedImgTagOpen) {
				super.startElement(uri, localName, qName, attributes);
			}
		}

		@Override
		public void characters(char[] ch, int start, int length) throws SAXException {

			// Swallow the text in between UUID start and close tags.
			if (!isEmbeddedAnchorTagOpen) {
				super.characters(ch, start, length);
			}
		}

		@Override
		public void endElement(String uri, String localName, String qName) throws SAXException {
			if (isEmbeddedAnchorTagOpen) {
				isEmbeddedAnchorTagOpen = false;
				return;
			}

			// Swallow the event if closing an embedded image tag.
			if (isEmbeddedImgTagOpen && IMG_TAG.equalsIgnoreCase(localName) && XHTML.equals(uri)) {
				return;
			}

			// Error state. Output this event and the previous one.
			if (isEmbeddedImgTagOpen) {
				super.startElement(uri, IMG_TAG, IMG_TAG, imgAtts);
				isEmbeddedImgTagOpen = false;
				imgAtts = null;
			}

			super.endElement(uri, localName, qName);
		}
	}

	protected class UUIDTokenResolver implements TokenResolver {

		public Reader resolveToken(String token) throws IOException {
			final Path path = pathMap.get(token);

			if (null == path) {
				return null;
			} else {
				//return new java.io.StringReader("testtest");
				return DataURIEncodingInputStream.createReader(path, metaMap.get(token));
			}
		}
	}
}
