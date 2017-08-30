package org.icij.extract.parser.afp;

import org.afplib.afplib.*;
import org.afplib.base.SF;
import org.afplib.io.AfpInputStream;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.XHTMLContentHandler;
import org.eclipse.emf.ecore.EObject;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.*;
import java.util.Set;

import static java.util.Collections.singleton;

/**
 * Parser for Advanced Function Presentation (AFP) files.
 */
public class AFPParser extends AbstractParser {

	private static final MediaType AFP_MIME_TYPE = MediaType.application("vnd.ibm.modcap");

	private static final Set<MediaType> SUPPORTED_TYPES = singleton(AFP_MIME_TYPE);
	private static final long serialVersionUID = -2246805926193406010L;

	@Override
	public Set<MediaType> getSupportedTypes(final ParseContext context) {
		return SUPPORTED_TYPES;
	}

	@Override
	public void parse(final InputStream stream, final ContentHandler handler, final Metadata metadata,
	                  final ParseContext context) throws IOException, SAXException, TikaException {
		final XHTMLContentHandler xhtml = new XHTMLContentHandler(handler, metadata);

		final AfpInputStream ain = new AfpInputStream(stream);
		final AFPVisitor visitor = new AFPVisitor(xhtml, metadata, context);

		SF sf;
		while ((sf = ain.readStructuredField()) != null) {
			System.out.println("iterate: " + sf.getClass().getCanonicalName());
			accept(visitor, sf);

			for (EObject eo : sf.eContents()) {
				System.out.println("  contents: " + eo.eClass().getInstanceClassName());
			}
		}
	}

	private void accept(final AFPVisitor visitor, final SF sf) throws SAXException, IOException {
		switch (sf.eClass().getClassifierID()) {
			case AfplibPackage.BBC: visitor.visit((BBC) sf);
			break;

			case AfplibPackage.BDA: visitor.visit((BDA) sf);
			break;

			case AfplibPackage.BDT: visitor.visit((BDT) sf);
			break;

			case AfplibPackage.BGR: visitor.visit((BGR) sf);
			break;

			case AfplibPackage.BIM: visitor.visit((BIM) sf);
			break;

			case AfplibPackage.BPG: visitor.visit((BPG) sf);
			break;

			case AfplibPackage.BPT: visitor.visit((BPT) sf);
			break;

			case AfplibPackage.EBC: visitor.visit((EBC) sf);
			break;

			case AfplibPackage.EDT: visitor.visit((EDT) sf);
			break;

			case AfplibPackage.EIM: visitor.visit((EIM) sf);
			break;

			case AfplibPackage.EPG: visitor.visit((EPG) sf);
			break;

			case AfplibPackage.EPT: visitor.visit((EPT) sf);
			break;

			case AfplibPackage.IDD: visitor.visit((IDD) sf);
			break;

			case AfplibPackage.IPD: visitor.visit((IPD) sf);
			break;

			case AfplibPackage.MCF: visitor.visit((MCF) sf);
			break;

			case AfplibPackage.NOP: visitor.visit((NOP) sf);
			break;

			case AfplibPackage.PTX: visitor.visit((PTX) sf);
			break;
		}
	}
}
