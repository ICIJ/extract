package org.icij.extract.parser.ocr;

import net.sourceforge.tess4j.ITesseract;
import net.sourceforge.tess4j.Tesseract1;
import net.sourceforge.tess4j.TesseractException;

import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TemporaryResources;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.mime.MediaTypeRegistry;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.CompositeParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.image.ImageParser;
import org.apache.tika.parser.image.TiffParser;
import org.apache.tika.parser.jpeg.JpegParser;
import org.apache.tika.sax.XHTMLContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.*;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Arrays.asList;

/**
 * Tess4JParser powered by Tesseract native libraries.
 */
public class Tess4JParser extends AbstractParser {

	private static final long serialVersionUID = -5693216478732659L;

	private static final Tess4JParserConfig DEFAULT_CONFIG = new Tess4JParserConfig();

	private static final Set<MediaType> SUPPORTED_TYPES = Collections.unmodifiableSet(
			new HashSet<>(asList(new MediaType[]{
					MediaType.image("png"),
					MediaType.image("jpeg"),
					MediaType.image("jpx"),
					MediaType.image("jp2"),
					MediaType.image("gif"),
					MediaType.image("tiff"),
					MediaType.image("x-ms-bmp"),
					MediaType.image("x-portable-pixmap")
			}))
	);

	// TIKA-1445 workaround parser.
	private static Parser _TMP_IMAGE_METADATA_PARSER = new CompositeImageParser();

	private static class CompositeImageParser extends CompositeParser {
		private static final long serialVersionUID = -1593574568521937L;

		private static List<Parser> imageParsers = asList(new Parser[] {
				new ImageParser(),
				new JpegParser(),
				new TiffParser()
		});

		CompositeImageParser() {
			super(new MediaTypeRegistry(), imageParsers);
		}
	}

	@Override
	public Set<MediaType> getSupportedTypes(final ParseContext context) {

		// If no Tess4JParserConfig set, don't advertise anything, so the other image parsers can be selected instead.
		if (context.get(Tess4JParserConfig.class) == null) {
			return Collections.emptySet();
		}

		// Otherwise offer supported image types.
		return SUPPORTED_TYPES;
	}

	@Override
	public void parse(final InputStream stream, final ContentHandler handler, final Metadata metadata,
	                  final ParseContext context) throws IOException, SAXException, TikaException {
		final Tess4JParserConfig config = context.get(Tess4JParserConfig.class, DEFAULT_CONFIG);
		final TemporaryResources tmp = new TemporaryResources();

		try (final TikaInputStream tis = TikaInputStream.get(stream, tmp)) {
			final long size = tis.getLength();

			if (size >= config.getMinFileSizeToOcr() && size <= config.getMaxFileSizeToOcr()) {
				extractOutput(new StringReader(doOCR(tis.getPath(), config)),
						new XHTMLContentHandler(handler, metadata));
			}

			// Temporary workaround for TIKA-1445 - until we can specify
			//  composite parsers with strategies (eg Composite, Try In Turn),
			//  always send the image onwards to the regular parser to have
			//  the metadata for them extracted as well
			_TMP_IMAGE_METADATA_PARSER.parse(tis, handler, metadata, context);
		} finally {
			tmp.dispose();
		}
	}

	/**
	 * Run Tess4J OCR instance.
	 *
	 * @param input  the File to be OCRed
	 * @param config the Configuration for tesseract-ocr
	 * @throws IOException if an input error occurred
	 */
	private String doOCR(final Path input, final Tess4JParserConfig config) throws IOException, TikaException {
		final ITesseract tesseract = new Tesseract1(); // JNA direct mapping.

		tesseract.setDatapath(config.getDataPath());
		tesseract.setLanguage(config.getLanguage());
		tesseract.setPageSegMode(config.getPageSegMode());
		tesseract.setOcrEngineMode(config.getOcrEngineMode());

		try {
			return tesseract.doOCR(input.toFile());
		} catch (TesseractException e) {
			throw new TikaException("Failed to OCR using Tess4J.", e);
		}
	}

	/**
	 * Reads the contents of the given stream and write it to the given XHTML
	 * content handler. The stream is closed once fully processed.
	 *
	 * @param reader string reader for the OCR result
	 * @param xhtml XHTML content handler
	 * @throws SAXException if the XHTML SAX events could not be handled
	 * @throws IOException if an input error occurred
	 */
	private void extractOutput(final Reader reader, final XHTMLContentHandler xhtml) throws SAXException, IOException {
		xhtml.startDocument();
		xhtml.startElement("div", "class", "ocr");

		final char[] buffer = new char[1024];

		for (int n = reader.read(buffer); n != -1; n = reader.read(buffer)) {
			if (n > 0) {
				xhtml.characters(buffer, 0, n);
			}
		}

		xhtml.endElement("div");
		xhtml.endDocument();
	}
}
