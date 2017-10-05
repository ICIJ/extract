package org.icij.extract.parser.afp;

import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TemporaryResources;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.ocr.TesseractOCRConfig;
import org.apache.tika.parser.ocr.TesseractOCRParser;

import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;

import java.util.*;

public class IOCAParser extends AbstractParser {

	private static final TesseractOCRConfig DEFAULT_CONFIG = new TesseractOCRConfig();
	private static final TesseractOCRParser DELEGATE = new TesseractOCRParser();

	private static final Set<MediaType> SUPPORTED_TYPES = new HashSet<>(Arrays.asList(
			MediaType.image("x-afp+fs10"), MediaType.image("x-afp+fs11"), MediaType.image("x-afp+fs45")
	));

	private static final long serialVersionUID = -2246805926193406010L;

	@Override
	public Set<MediaType> getSupportedTypes(final ParseContext context) {

		// Check if Tesseract is installed.
		if (DELEGATE.hasTesseract(context.get(TesseractOCRConfig.class, DEFAULT_CONFIG))) {
			return SUPPORTED_TYPES;
		}

		// Otherwise don't advertise anything, so the other image parsers can be selected instead.
		return Collections.emptySet();
	}

	@Override
	public void parse(final InputStream in, final ContentHandler handler, final Metadata metadata,
	                  final ParseContext context) throws IOException, SAXException, TikaException {
		final Iterator<ImageReader> readers = ImageIO.getImageReadersByFormatName("ica");

		if (!readers.hasNext()) {
			throw new TikaException("No IOCA reader plugged in.");
		}

		final ImageReader reader = readers.next();

		try (final TemporaryResources tmp = new TemporaryResources();
		     final TikaInputStream tis = TikaInputStream.get(in, tmp);
		     final ImageInputStream iis = ImageIO.createImageInputStream(tis.getFile())) {

			reader.setInput(iis);

			for (int i = 0; i < reader.getNumImages(true); i++) {
				final File file = tmp.createTemporaryFile();

				ImageIO.write(reader.read(i), "png", file);
				DELEGATE.parse(Files.newInputStream(file.toPath()), handler, metadata, context);
			}
		} finally {
			reader.dispose();
		}
	}
}
