package org.icij.extract.parser.afp;

import org.afplib.afplib.*;
import org.afplib.base.Triplet;
import org.afplib.io.SDFHelper;
import org.apache.tika.extractor.EmbeddedDocumentUtil;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.PagedText;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.EmbeddedContentHandler;
import org.apache.tika.sax.XHTMLContentHandler;
import org.eclipse.emf.ecore.EObject;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.LinkedList;
import java.util.List;

/**
 * A visitor class for different AFP structured fields.
 *
 * See
 * <a href="https://www.ibm.com/support/knowledgecenter/en/SSLTBW_1.13.0/com.ibm.zos.r13.admb100/admb1a04722.htm">
 *     Structured field formats</a> for a description of each one.
 */
class AFPVisitor {

	private final XHTMLContentHandler xhtml;
	private final Metadata metadata;
	private final ParseContext context;

	private final List<IPD> pictureData = new LinkedList<>();
	private Metadata imageMetadata = null;
	private String imageName = null;

	private int pages = 0;
	private int images = 0;

	AFPVisitor(final XHTMLContentHandler xhtml, final Metadata metadata, final ParseContext context) {
		this.xhtml = xhtml;
		this.metadata = metadata;
		this.context = context;
	}

	/**
	 * Indicates the beginning of a bar code object.
	 *
	 * @param bbc begin bar code object
	 */
	void visit(final BBC bbc) throws SAXException {
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
	void visit(final BDA bda) throws SAXException {
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
	void visit(final BDT bdt) {
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
	void visit(final BGR bgr) throws SAXException {
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
	void visit(final BIM bim) throws SAXException {
		imageMetadata = new Metadata();
		imageName = bim.getIdoName();

		if (null != imageName) {
			imageName = imageName.trim();
		}

		if (null == imageName || imageName.isEmpty()) {
			imageName = "image-" + (images++);
		}

		imageMetadata.set(TikaCoreProperties.EMBEDDED_RESOURCE_TYPE,
				TikaCoreProperties.EmbeddedResourceType.INLINE.toString());

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
	void visit(final BPG bpg) throws SAXException {
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
	void visit(final BPT bpt) throws SAXException {
		xhtml.startElement("p");
	}

	/**
	 * Indicates the end of a bar code object.
	 *
	 * @param ebc end bar code object
	 */
	void visit(final EBC ebc) throws SAXException {
		xhtml.endElement("div");
	}

	/**
	 * Indicates the end of the document.
	 *
	 * @param edt end document
	 */
	void visit(final EDT edt) {
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
	void visit(final EIM eim) throws IOException, SAXException {
		final Triplet[] triplets = SDFHelper.ipds2sdf(pictureData);
		final ByteArrayOutputStream boas = new ByteArrayOutputStream();

		int compressionId = 0;
		int bitOrder = 0;
		int bitsPerIDE = 0;

		final byte[] buffer;
		String type = null;
		String suffix = null;

		pictureData.clear();

		for (Triplet triplet: triplets) {
			final int classifierId = triplet.eClass().getClassifierID();

			switch (classifierId) {
				default:
					System.out.println("  classifier: " + Integer.toString(classifierId) + " / " +
							Integer.toHexString(classifierId).toUpperCase());
					break;

				case AfplibPackage.BEGIN_SEGMENT:
					final BeginSegment beginSegment = (BeginSegment) triplet;

					System.out.println("    segment name: " + beginSegment.getSEGNAME());
					break;

				case AfplibPackage.END_SEGMENT:
					break;

				case AfplibPackage.BEGIN_IMAGE:
					final BeginImage beginImage = (BeginImage) triplet;

					if (beginImage.getOBJTYPE() != 0xFF) {
						throw new IOException("Object type must be equal to xFF (IOCA image object).");
					}

					break;

				case AfplibPackage.END_IMAGE:
					break;

				case AfplibPackage.IMAGE_SIZE:
					final ImageSize imageSize = (ImageSize) triplet;

					System.out.println("    hresol: " + imageSize.getHRESOL());
					System.out.println("    hsize: " + imageSize.getHSIZE());
					System.out.println("    unitbase: " + imageSize.getUNITBASE());
					System.out.println("    vresol: " + imageSize.getVRESOL());
					System.out.println("    vsize: " + imageSize.getVSIZE());
					break;

				case AfplibPackage.IDE_SIZE:
					final IDESize ideSize = (IDESize) triplet;

					bitsPerIDE = ideSize.getIDESZ();

					System.out.println("    IDE size: " + Integer.toHexString(bitsPerIDE));
					break;

				case AfplibPackage.IMAGE_ENCODING:
					final ImageEncoding imageEncoding = (ImageEncoding) triplet;

					bitOrder = imageEncoding.getBITORDR();
					compressionId = imageEncoding.getCOMPRID();

					System.out.println("    bit order: " + Integer.toHexString(bitOrder));
					System.out.println("    compression ID: " + Integer.toHexString(compressionId));
					break;

				case AfplibPackage.IMAGE_DATA:
					boas.write(((ImageData) triplet).getDATA());
					break;
			}
		}

		type = compressIdToType(compressionId);
		suffix = suffixForType(type);

		imageName = imageName + suffix;

		imageMetadata.set(Metadata.CONTENT_TYPE, type);
		imageMetadata.set(Metadata.RESOURCE_NAME_KEY, imageName);

		buffer = boas.toByteArray();

		Files.write(Paths.get("/Users/matt/Downloads/test/" + imageName), buffer, StandardOpenOption.CREATE);

		try (final InputStream in = TikaInputStream.get(buffer)) {
			EmbeddedDocumentUtil.getEmbeddedDocumentExtractor(context).parseEmbedded(in, new
					EmbeddedContentHandler(xhtml), imageMetadata, false);
		}

		imageMetadata = null;
	}

	/**
	 * Indicates the end of a page.
	 *
	 * @param epg end page
	 * @throws SAXException if there's an exception writing the closing tag
	 */
	void visit(final EPG epg) throws SAXException {
		xhtml.endElement("div");
	}

	/**
	 * Indicates the end of a presentation text object.
	 *
	 * @param ept end presentation text
	 */
	void visit(final EPT ept) throws SAXException {
		xhtml.endElement("p");
	}

	/**
	 * Specifies the size of the image to be included.
	 *
	 * @param idd image data descriptor
	 */
	void visit(final IDD idd) throws IOException {
		for (EObject eo: idd.eContents()) {
			if (eo instanceof IOCAFunctionSetIdentification) {
				final IOCAFunctionSetIdentification identification = (IOCAFunctionSetIdentification) eo;
				final int functionSet = identification.getFCNSET();

				System.out.println("  function set: " + functionSet + " / " +
						Integer.toHexString(functionSet).toUpperCase());

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
	void visit(final IPD ipd) throws IOException {
		pictureData.add(ipd);
	}

	/**
	 * Identifies the correspondence between external font names and a resource local identifier.
	 *
	 * @param mcf map coded font
	 */
	void visit(final MCF mcf) {

	}

	/**
	 * This may be used to add comments to the data stream.
	 *
	 * @param nop no operation
	 */
	void visit(final NOP nop) throws SAXException {
		final byte[] comment = nop.getUndfData();

		if (null != comment && comment.length > 0) {
			xhtml.startElement("p");
			xhtml.characters(new String(comment, nop.getCharset()));
			xhtml.endElement("p");
		}
	}

	/**
	 * This contains a chain of text controls.
	 *
	 * @param ptx presentation text data
	 */
	void visit(final PTX ptx) {

	}

	private String compressIdToType(final int compressionId) {
		switch (compressionId) {

			// G4 MMR-Modified Modified READ
			// (ITU-TSS T.6 Group 4 two-dimensional coding standard for facsimile)
			case 0x82:
				return "image/tiff";

			case 0x83:
				return "image/jpeg";

			case 0x84:
				return "image/jbig2";
		}

		throw new IllegalArgumentException("Unknown compression ID: " + Integer.toHexString(compressionId));
	}

	private String suffixForType(final String type) {
		switch (type) {
			case "image/tiff":
				return ".tiff";

			case "image/jpeg":
				return ".jpg";

			case "image/jbig2":
				return ".jb2";
		}

		throw new IllegalArgumentException("Unknown type: " + type);
	}
}
