package org.icij.extract.parser.afp;

import org.afplib.afplib.*;
import org.afplib.afplib.impl.BPGImpl;
import org.afplib.base.SF;
import org.afplib.io.AfpInputStream;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.CloseShieldInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.PagedText;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.XHTMLContentHandler;
import org.eclipse.emf.ecore.EObject;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.util.Set;

import static java.util.Collections.singleton;

/**
 * Parser for Advanced Function Presentation (AFP) files.
 */
public class Yan74AFPParser extends AbstractParser {

	private static final MediaType AFP_MIMETYPE = MediaType.application("vnd.ibm.modcap");

	private static final Set<MediaType> SUPPORTED_TYPES = singleton(AFP_MIMETYPE);
	private static final long serialVersionUID = -2246805926193406010L;

	@Override
	public Set<MediaType> getSupportedTypes(final ParseContext context) {
		return SUPPORTED_TYPES;
	}

	@Override
	public void parse(final InputStream stream, final ContentHandler handler, final Metadata metadata,
	                  final ParseContext context) throws IOException, SAXException, TikaException {
		final XHTMLContentHandler xhtml = new XHTMLContentHandler(handler, metadata);

		parseAFP(stream, xhtml, metadata);
	}

	private void parseAFP(final InputStream input, final XHTMLContentHandler xhtml, final Metadata metadata)
			throws IOException, SAXException {
		final AfpInputStream ain = new AfpInputStream(input);

//		final BDT bdt = (BDT) ain.readStructuredField();
//		final EDT edt = (EDT) ain.readStructuredField();
//
//		metadata.set(TikaCoreProperties.TITLE, bdt.getDocName());
//		System.out.println("Title: " + bdt.getDocName());

		int pages = 0;
		String title = null;

		SF sf;
		while ((sf = ain.readStructuredField()) != null) {
			System.out.println("iterate: " + sf.getClass().getCanonicalName());

			for (EObject eo : sf.eContents()) {
				System.out.println(eo.eClass().getInstanceClassName());
			}
		}

		metadata.set(PagedText.N_PAGES, pages);
	}

	/**
	 * A visitor class for different AFP structured fields.
	 *
	 * See
	 * <a href="https://www.ibm.com/support/knowledgecenter/en/SSLTBW_1.13.0/com.ibm.zos.r13.admb100/admb1a04722.htm">
	 *     Structured field formats</a> for a description of each one.
	 */
	private static class AFPVisitor {

		private final XHTMLContentHandler xhtml;
		private final Metadata metadata;

		private int pages = 0;
		private int images = 0;

		AFPVisitor(final XHTMLContentHandler xhtml, final Metadata metadata) {
			this.xhtml = xhtml;
			this.metadata = metadata;
		}

		void visit(final BDT bdt) {
			String title = bdt.getDocName();

			if (null != title) {
				title = title.trim();
			}

			if (null != title && !title.isEmpty()) {
				metadata.set(TikaCoreProperties.TITLE, title);
			}
		}

		void visit(final EDT edt) {
			String title = edt.getDocName();

			if (null != title) {
				title = title.trim();
			}

			if (null != title && !title.isEmpty()) {
				metadata.set(TikaCoreProperties.TITLE, title);
			}

			metadata.set(PagedText.N_PAGES, pages);
		}

		void visit(final BPG bpg) throws SAXException {
			pages++;
			xhtml.startElement("div", "class", "page");
		}

		void visit(final EPG epg) throws SAXException {
			xhtml.endElement("div");
		}

		/**
		 * Begin image object.
		 *
		 * @param bim the image object structured field
		 * @throws SAXException if there's an exception writing the img tag
		 */
		void visit(final BIM bim) throws SAXException {
			final Metadata embeddedMetadata = new Metadata();
			String imageName = bim.getIdoName();

			if (null != imageName) {
				imageName = imageName.trim();
			}

			if (null == imageName || imageName.isEmpty()) {
				imageName = "image-" + (images++) + ".";
			}

			embeddedMetadata.set(Metadata.RESOURCE_NAME_KEY, imageName);
		}
	}
}
