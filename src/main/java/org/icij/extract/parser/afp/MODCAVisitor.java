package org.icij.extract.parser.afp;

import org.afplib.afplib.*;
import org.afplib.base.SF;

import org.apache.tika.extractor.EmbeddedDocumentExtractor;
import org.apache.tika.extractor.EmbeddedDocumentUtil;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.PagedText;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.XHTMLContentHandler;

import org.eclipse.emf.ecore.EObject;

import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import java.util.LinkedList;
import java.util.List;

/**
 * A visitor class for different AFP structured fields.
 *
 * See
 * <a href="https://www.ibm.com/support/knowledgecenter/en/SSLTBW_1.13.0/com.ibm.zos.r13.admb100/admb1a04722.htm">
 *     Structured field formats</a> for a description of each one.
 */
class MODCAVisitor {

	private static final boolean DEBUG = false;

	private final XHTMLContentHandler xhtml;
	private final Metadata metadata;
	private final ParseContext context;

	private final List<IPD> pictureData = new LinkedList<>();
	private String imageName = null;

	private int functionSet = 0;
	private int pages = 0;
	private int images = 0;

	MODCAVisitor(final XHTMLContentHandler xhtml, final Metadata metadata, final ParseContext context) {
		this.xhtml = xhtml;
		this.metadata = metadata;
		this.context = context;
	}

	void accept(final SF sf) throws SAXException, IOException {
		switch (sf.eClass().getClassifierID()) {
			case AfplibPackage.BBC: visit((BBC) sf);
				break;
			case AfplibPackage.BDA: visit((BDA) sf);
				break;
			case AfplibPackage.BDT: visit((BDT) sf);
				break;
			case AfplibPackage.BGR: visit((BGR) sf);
				break;
			case AfplibPackage.BIM: visit((BIM) sf);
				break;
			case AfplibPackage.BPG: visit((BPG) sf);
				break;
			case AfplibPackage.BPT: visit((BPT) sf);
				break;
			case AfplibPackage.EBC: visit((EBC) sf);
				break;
			case AfplibPackage.EDT: visit((EDT) sf);
				break;
			case AfplibPackage.EIM: visit((EIM) sf);
				break;
			case AfplibPackage.EPG: visit((EPG) sf);
				break;
			case AfplibPackage.EPT: visit((EPT) sf);
				break;
			case AfplibPackage.IDD: visit((IDD) sf);
				break;
			case AfplibPackage.IPD: visit((IPD) sf);
				break;
			case AfplibPackage.MCF: visit((MCF) sf);
				break;
			case AfplibPackage.NOP: visit((NOP) sf);
				break;
			case AfplibPackage.PTX: visit((PTX) sf);
				break;
		}
	}

	/**
	 * Indicates the beginning of a bar code object.
	 *
	 * @param bbc begin bar code object
	 */
	private void visit(final BBC bbc) throws SAXException {
		final String name = bbc.getBCdoName();

		xhtml.startElement("div", "class", "bar-code");

		if (null != name) {
			xhtml.startElement("p");
			xhtml.characters(name.trim());
			xhtml.endElement("p");
		}
	}

	/**
	 * Contains data parameters for positioning, encoding, and presenting a bar code symbol.
	 *
	 * @param bda bar code data
	 * @throws SAXException if there's an exception writing the tag
	 */
	private void visit(final BDA bda) throws SAXException {
		final byte[] data = bda.getData();

		if (null != data && data.length > 0) {
			xhtml.startElement("p");
			xhtml.characters(new String(data, bda.getCharset()));
			xhtml.endElement("p");
		}
	}

	/**
	 * Indicates the beginning of the document.
	 *
	 * @param bdt begin document
	 */
	private void visit(final BDT bdt) {
		final String title = bdt.getDocName();

		if (null != title) {
			metadata.set(TikaCoreProperties.TITLE, title.trim());
		}
	}

	/**
	 * Indicates the beginning of a graphics object.
	 *
	 * @param bgr begin graphics object
	 */
	private void visit(final BGR bgr) throws SAXException {
		final String name = bgr.getGdoName();

		if (null != name) {
			xhtml.startElement("p");
			xhtml.characters(name.trim());
			xhtml.endElement("p");
		}
	}

	/**
	 * Indicates the beginning of an image object.
	 *
	 * @param bim begin image object
	 * @throws SAXException if there's an exception writing the image tag
	 */
	private void visit(final BIM bim) throws SAXException {
		imageName = bim.getIdoName();

		if (null != imageName) {
			imageName = imageName.trim();
		}

		if (null == imageName || imageName.isEmpty()) {
			imageName = "image-" + (images++);
		}

		final AttributesImpl attr = new AttributesImpl();

		attr.addAttribute("", "src", "src", "CDATA", "embedded:" + imageName);
		attr.addAttribute("", "alt", "alt", "CDATA", imageName);
		xhtml.startElement("img", attr);
		xhtml.endElement("img");
	}

	/**
	 * Indicates the beginning of a page.
	 *
	 * @param bpg begin page
	 * @throws SAXException if there's an exception writing the page tag
	 */
	private void visit(final BPG bpg) throws SAXException {
		final String name = bpg.getPageName();

		pages++;
		xhtml.startElement("div", "class", "page");

		if (null != name) {
			xhtml.startElement("p");
			xhtml.characters(name.trim());
			xhtml.endElement("p");
		}
	}

	/**
	 * Indicates the beginning of a presentation text object.
	 *
	 * @param bpt begin presentation text
	 */
	private void visit(final BPT bpt) throws SAXException {
		xhtml.startElement("p");
	}

	/**
	 * Indicates the end of a bar code object.
	 *
	 * @param ebc end bar code object
	 */
	private void visit(final EBC ebc) throws SAXException {
		xhtml.endElement("div");
	}

	/**
	 * Indicates the end of the document.
	 *
	 * @param edt end document
	 */
	private void visit(final EDT edt) {
		final String title = edt.getDocName();

		if (null != title) {
			metadata.set(TikaCoreProperties.TITLE, title.trim());
		}

		metadata.set(PagedText.N_PAGES, pages);
	}

	/**
	 * Indicates the end of an image object.
	 *
	 * @param eim end image object
	 */
	private void visit(final EIM eim) throws IOException, SAXException {
		final EmbeddedDocumentExtractor embedExtractor;
		final Metadata metadata = new Metadata();

		metadata.set(TikaCoreProperties.EMBEDDED_RESOURCE_TYPE,
				TikaCoreProperties.EmbeddedResourceType.INLINE.toString());
		metadata.set(TikaCoreProperties.TITLE, imageName);

		switch (functionSet) {
			case 10:
			case 11:
			case 40:
			case 42:
			case 45:
				metadata.set(Metadata.CONTENT_TYPE, "image/x-afp+fs" + functionSet);
				break;

			default:
				throw new IOException("Unknown function set: " + functionSet + ".");
		}

		embedExtractor = EmbeddedDocumentUtil.getEmbeddedDocumentExtractor(context);
		if (embedExtractor.shouldParseEmbedded(metadata)) {
			final ByteArrayOutputStream bos = new ByteArrayOutputStream(4096);

			for (IPD ipd : pictureData) {
				bos.write(ipd.getIOCAdat());
			}

			embedExtractor.parseEmbedded(new ByteArrayInputStream(bos.toByteArray()),
					xhtml, metadata, false);
		}
	}

	/**
	 * Indicates the end of a page.
	 *
	 * @param epg end page
	 * @throws SAXException if there's an exception writing the closing tag
	 */
	private void visit(final EPG epg) throws SAXException {
		xhtml.endElement("div");
	}

	/**
	 * Indicates the end of a presentation text object.
	 *
	 * @param ept end presentation text
	 */
	private void visit(final EPT ept) throws SAXException {
		xhtml.endElement("p");
	}

	/**
	 * Specifies the size of the image to be included.
	 *
	 * @param idd image data descriptor
	 */
	private void visit(final IDD idd) throws IOException {
		if (DEBUG) {
			System.out.println("\txresol: " + idd.getXRESOL());
			System.out.println("\txsize: " + idd.getXSIZE());
			System.out.println("\tunitbase: " + idd.getUNITBASE());
			System.out.println("\tyresol: " + idd.getYRESOL());
			System.out.println("\tysize: " + idd.getYSIZE());
			System.out.println("\tnum sdfs: " + idd.getSDFS().size());
		}

		for (EObject eo: idd.eContents()) {
			if (eo instanceof IOCAFunctionSetIdentification) {
				final IOCAFunctionSetIdentification identification = (IOCAFunctionSetIdentification) eo;
				functionSet = identification.getFCNSET();

				if (DEBUG) {
					System.out.println("\tfunction set: " + functionSet + " / " +
							Integer.toHexString(functionSet).toUpperCase());
				}

				if (identification.getCATEGORY() != 1) {
					throw new IOException("IOCA function set category must be equal to x01.");
				}
			}
		}
	}

	/**
	 * Contains the image orders to be drawn.
	 *
	 * @param ipd image picture data
	 */
	private void visit(final IPD ipd) throws IOException {
		pictureData.add(ipd);
	}

	/**
	 * Identifies the correspondence between external font names and a resource local identifier.
	 *
	 * @param mcf map coded font
	 */
	private void visit(final MCF mcf) {

	}

	/**
	 * This may be used to add comments to the data stream.
	 *
	 * @param nop no operation
	 */
	private void visit(final NOP nop) throws SAXException {
		final byte[] comment = nop.getUndfData();

		if (null != comment && comment.length > 0) {
			xhtml.startElement("p");

			if (null != nop.getCharset()) {
				xhtml.characters(new String(comment, nop.getCharset()));
			} else {
				xhtml.characters(new String(comment));
			}

			xhtml.endElement("p");
		}
	}

	/**
	 * This contains a chain of text controls.
	 *
	 * @param ptx presentation text data
	 */
	private void visit(final PTX ptx) {

	}
}
