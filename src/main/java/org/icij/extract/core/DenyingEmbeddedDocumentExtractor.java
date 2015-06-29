package org.icij.extract.core;

import java.io.InputStream;
import java.io.IOException;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.extractor.EmbeddedDocumentExtractor;

import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

/**
 * A custom extractor that prevents Tika from parsing any embedded documents.
 *
 * @since 1.0.0-beta
 */
public class DenyingEmbeddedDocumentExtractor implements EmbeddedDocumentExtractor {

	public boolean shouldParseEmbedded(Metadata metadata) {
		return false;
	}

	public void parseEmbedded(InputStream stream, ContentHandler handler, Metadata metadata, boolean outputHtml) throws SAXException, IOException {
		final IOException e = new IOException("");
		e.initCause(new IllegalStateException("A parser illegally attempted to parse an embedded document."));
		throw e;
	}
}
