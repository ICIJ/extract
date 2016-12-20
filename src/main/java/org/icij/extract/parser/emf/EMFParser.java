package org.icij.extract.parser.emf;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.XHTMLContentHandler;
import org.freehep.graphicsio.emf.EMFInputStream;
import org.freehep.graphicsio.emf.EMFTag;
import org.freehep.graphicsio.emf.gdi.*;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.freehep.util.io.Tag;
import org.xml.sax.helpers.AttributesImpl;

import static org.apache.tika.sax.XHTMLContentHandler.XHTML;

public class EMFParser extends AbstractParser {

	private static final Set<MediaType> SUPPORTED_TYPES = Collections.unmodifiableSet(new HashSet<>(Arrays
			.asList(MediaType.application("x-emf"), MediaType.image("emf"))));
	private static final String EMF_MIME_TYPE = "image/emf";
	private static final long serialVersionUID = 5516989102471431040L;

	@Override
	public Set<MediaType> getSupportedTypes(final ParseContext context) {
		return SUPPORTED_TYPES;
	}

	@Override
	public void parse(final InputStream stream, final ContentHandler handler, final Metadata metadata, final
	ParseContext context) throws IOException, SAXException, TikaException {
		metadata.set(Metadata.CONTENT_TYPE, EMF_MIME_TYPE);

		final EMFInputStream input = new EMFInputStream(stream);
		Tag tag;

		final XHTMLContentHandler xhtml = new XHTMLContentHandler(handler, metadata);
		xhtml.startDocument();

		while ((tag = input.readTag()) != null) {
			if (!(tag instanceof EMFTag)) {
				continue;
			}

			xhtml.startElement(XHTML, "p", "p", new AttributesImpl());

			if ((tag instanceof ExtTextOutA) || (tag instanceof ExtTextOutW)) {
				xhtml.characters(((AbstractExtTextOut) tag).getText().getString());
			}

			xhtml.endElement(XHTML, "p", "p");
		}

		xhtml.endDocument();

		input.close();
	}
}
