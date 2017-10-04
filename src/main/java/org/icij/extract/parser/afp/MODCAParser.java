package org.icij.extract.parser.afp;

import org.afplib.base.SF;
import org.afplib.io.AfpInputStream;

import org.eclipse.emf.ecore.EObject;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.XHTMLContentHandler;

import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.*;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Parser for Advanced Function Presentation (AFP) files.
 */
public class MODCAParser extends AbstractParser {

	private static final boolean DEBUG = false;

	private static final Set<MediaType> SUPPORTED_TYPES = new HashSet<>(Arrays.asList(
			MediaType.application("x-afp"),
			MediaType.application("vnd.ibm.modcap")
	));

	private static final long serialVersionUID = -2246805926193406010L;

	@Override
	public Set<MediaType> getSupportedTypes(final ParseContext context) {
		return SUPPORTED_TYPES;
	}

	@Override
	public void parse(final InputStream in, final ContentHandler handler, final Metadata metadata,
	                  final ParseContext context) throws IOException, SAXException, TikaException {
		final XHTMLContentHandler xhtml = new XHTMLContentHandler(handler, metadata);

		final AfpInputStream ain = new AfpInputStream(in);
		final MODCAVisitor visitor = new MODCAVisitor(xhtml, metadata, context);

		SF sf;
		while ((sf = ain.readStructuredField()) != null) {
			if (DEBUG) {
				System.out.println("iterate: " + sf.getClass().getCanonicalName());
			}

			visitor.accept(sf);

			if (DEBUG) {
				for (EObject eo : sf.eContents()) {
					System.out.println("\tcontents: " + eo.eClass().getInstanceClassName());
				}
			}
		}
	}
}
