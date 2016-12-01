package org.icij.extract.extractor;

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

	@Override
	public boolean shouldParseEmbedded(final Metadata metadata) {
		return false;
	}

	@Override
	public void parseEmbedded(final InputStream stream, final ContentHandler handler, final Metadata metadata, final
	boolean outputHtml) throws SAXException, IOException {
		throw new IllegalStateException("A parser illegally attempted to parse an embedded document.");
	}
}
