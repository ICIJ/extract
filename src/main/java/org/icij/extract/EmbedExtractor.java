package org.icij.extract;

import java.util.logging.Level;
import java.util.logging.Logger;

import java.io.InputStream;
import java.io.IOException;

import java.nio.file.Path;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.DelegatingParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.extractor.ParsingEmbeddedDocumentExtractor;
import org.apache.tika.io.TemporaryResources;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.io.CloseShieldInputStream;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.EmbeddedContentHandler;
import org.apache.tika.exception.TikaException;
import org.apache.tika.exception.EncryptedDocumentException;

import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */

public class EmbedExtractor extends ParsingEmbeddedDocumentExtractor {
	private final Logger logger;
	private final Path file;

	private final ParseContext context;

	public EmbedExtractor(Logger logger, Path file, ParseContext context) {
		super(context);
		this.logger = logger;
		this.file = file;
		this.context = context;
	}

	// This override is made for logging purposes, as by default exceptions are swallowed by ParsingEmbeddedDocumentExtractor.
	// See `extractImages` here:
	// http://svn.apache.org/viewvc/tika/trunk/tika-parsers/src/main/java/org/apache/tika/parser/pdf/PDF2XHTML.java?view=markup
	// And `parseEmbedded` here:
	// https://svn.apache.org/repos/asf/tika/trunk/tika-core/src/main/java/org/apache/tika/extractor/ParsingEmbeddedDocumentExtractor.java
	@Override
	public void parseEmbedded(InputStream stream, ContentHandler handler, Metadata metadata, boolean outputHtml) throws SAXException, IOException {
		logger.info("Extracting embedded document from document: " + file + ".");

		TemporaryResources tmp = new TemporaryResources();
		try {
			final TikaInputStream newStream = TikaInputStream.get(new CloseShieldInputStream(stream), tmp);

			if (stream instanceof TikaInputStream) {
				final Object container = ((TikaInputStream) stream).getOpenContainer();

				if (container != null) {
					newStream.setOpenContainer(container);
				}
			}

			new DelegatingParser().parse(newStream, new EmbeddedContentHandler(new BodyContentHandler(handler)), metadata, context);
		} catch (EncryptedDocumentException e) {
			logger.log(Level.WARNING, "Encrypted document embedded in document: " + file + ".", e);
		} catch (TikaException e) {
			logger.log(Level.WARNING, "Unable to parse embedded document in document: " + file + ".", e);
		} finally {
			tmp.close();
		}
	}
}
