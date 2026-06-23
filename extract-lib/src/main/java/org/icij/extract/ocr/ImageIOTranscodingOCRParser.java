package org.icij.extract.ocr;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TIFF;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * OCRs image types that an ImageIO reader can decode but the Tesseract OCR pipeline does not
 * handle directly (e.g. {@code image/x-jbig2}). Such images are decoded to a {@link BufferedImage},
 * re-encoded as PNG, and passed to the configured OCR parser.
 *
 * <p>Standard raster types (PNG/JPEG/TIFF/BMP/GIF/JP2) are deliberately excluded: they are already
 * OCR'd through Tika's image-parser + Tesseract path, so this parser never claims them.
 */
public class ImageIOTranscodingOCRParser implements Parser {
    private static final Logger LOGGER = LoggerFactory.getLogger(ImageIOTranscodingOCRParser.class);

    // Image media types already OCR'd through the standard image + Tesseract pipeline,
    // including known aliases. Anything ImageIO can read that is NOT here is transcoded and OCR'd.
    private static final Set<String> ALREADY_OCRABLE = Set.of(
            "image/png", "image/x-png",
            "image/jpeg",
            "image/tiff",
            "image/bmp", "image/x-bmp", "image/x-windows-bmp",
            "image/gif",
            "image/jp2", "image/jpx", "image/jpeg2000");

    private final Parser ocrParser;
    private final Set<MediaType> supportedTypes;

    public ImageIOTranscodingOCRParser(final Parser ocrParser) {
        this.ocrParser = ocrParser;
        this.supportedTypes = computeSupportedTypes(ocrParser);
    }

    private static Set<MediaType> computeSupportedTypes(final Parser ocrParser) {
        final Set<String> excluded = new HashSet<>(ALREADY_OCRABLE);
        ocrParser.getSupportedTypes(new ParseContext()).forEach(t -> excluded.add(t.toString()));
        final Set<MediaType> result = new HashSet<>();
        for (final String mime : ImageIO.getReaderMIMETypes()) {
            if (mime == null || mime.isBlank() || excluded.contains(mime)) {
                continue;
            }
            result.add(MediaType.parse(mime));
        }
        return Collections.unmodifiableSet(result);
    }

    @Override
    public Set<MediaType> getSupportedTypes(final ParseContext context) {
        return supportedTypes;
    }

    @Override
    public void parse(final InputStream stream, final ContentHandler handler, final Metadata metadata,
                      final ParseContext context) throws IOException, SAXException, TikaException {
        final BufferedImage image = decode(stream, metadata.get(Metadata.CONTENT_TYPE));
        if (image == null) {
            LOGGER.warn("could not decode image of type {} for OCR; emitting no content",
                    metadata.get(Metadata.CONTENT_TYPE));
            return;
        }
        metadata.set(TIFF.IMAGE_WIDTH, image.getWidth());
        metadata.set(TIFF.IMAGE_LENGTH, image.getHeight());
        final ByteArrayOutputStream png = new ByteArrayOutputStream();
        ImageIO.write(image, "png", png);
        metadata.set(Metadata.CONTENT_TYPE, "image/png");
        ocrParser.parse(new ByteArrayInputStream(png.toByteArray()), handler, metadata, context);
    }

    private static BufferedImage decode(final InputStream stream, final String contentType) throws IOException {
        final byte[] bytes = stream.readAllBytes();
        // Drive readers explicitly: ImageIO.read() relies on SPI format-sniffing that fails on the
        // headerless (PDF-ready) JBIG2 streams Tika emits for embedded images.
        final List<ImageReader> readers = new ArrayList<>();
        if (contentType != null) {
            final Iterator<ImageReader> it = ImageIO.getImageReadersByMIMEType(contentType);
            while (it.hasNext()) {
                readers.add(it.next());
            }
        }
        for (final ImageReader reader : readers) {
            try (ImageInputStream iis = ImageIO.createImageInputStream(new ByteArrayInputStream(bytes))) {
                reader.setInput(iis);
                return reader.read(0);
            } catch (final Exception e) {
                LOGGER.debug("reader {} failed to decode image: {}", reader.getClass().getName(), e.toString());
            } finally {
                reader.dispose();
            }
        }
        return null;
    }
}
