package org.icij.extract.ocr;

import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.apache.tika.metadata.TikaCoreProperties.CONTENT_TYPE_PARSER_OVERRIDE;
import static org.apache.tika.metadata.TikaCoreProperties.RESOURCE_NAME_KEY;

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

    // The ImageIO reader registry is JVM-stable, so scan it once rather than on every construction
    // (getReaderMIMETypes acquires a global lock on the IIORegistry).
    private static final List<String> READER_MIME_TYPES = Arrays.stream(ImageIO.getReaderMIMETypes())
            .filter(mime -> mime != null && !mime.isBlank())
            .distinct()
            .toList();

    private final Parser ocrParser;
    private final Set<MediaType> supportedTypes;

    public ImageIOTranscodingOCRParser(final Parser ocrParser) {
        this.ocrParser = ocrParser;
        this.supportedTypes = computeSupportedTypes(ocrParser);
    }

    // Claims every type an ImageIO reader can decode, minus those already OCR'd by the normal
    // pipeline (ALREADY_OCRABLE + the delegate's own types). This is a general fallback, not
    // JBIG2-only: today it resolves to JBIG2, WBMP, PCX and the non-PPM PNM variants.
    private static Set<MediaType> computeSupportedTypes(final Parser ocrParser) {
        final Set<String> excluded = new HashSet<>(ALREADY_OCRABLE);
        ocrParser.getSupportedTypes(new ParseContext()).forEach(t -> excluded.add(t.getBaseType().toString()));
        final Set<MediaType> result = new HashSet<>();
        for (final String mime : READER_MIME_TYPES) {
            if (!excluded.contains(mime)) {
                result.add(MediaType.parse(mime));
            }
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
        LOGGER.debug("transcoding {} (resource: {}) to PNG for OCR",
                metadata.get(Metadata.CONTENT_TYPE), metadata.get(RESOURCE_NAME_KEY));
        final BufferedImage image = decode(stream, metadata.get(Metadata.CONTENT_TYPE));
        if (image == null) {
            LOGGER.debug("could not decode image of type {} (resource: {}) for OCR; emitting no content",
                    metadata.get(Metadata.CONTENT_TYPE), metadata.get(RESOURCE_NAME_KEY));
            return;
        }
        metadata.set(TIFF.IMAGE_WIDTH, image.getWidth());
        metadata.set(TIFF.IMAGE_LENGTH, image.getHeight());

        // Spool the re-encoded PNG to a temp file instead of holding it in memory: avoids a second
        // full-size byte[] copy and hands the OCR engine a file-backed stream (Tesseract needs a
        // file on disk anyway). The OCR delegate derives its image format from
        // CONTENT_TYPE_PARSER_OVERRIDE, so it must be set alongside CONTENT_TYPE — otherwise the
        // Tess4J path cannot resolve an extension and silently fails.
        final Path pngFile = Files.createTempFile("extract-ocr-transcode-", ".png");
        try {
            try (OutputStream out = Files.newOutputStream(pngFile)) {
                if (!ImageIO.write(image, "png", out)) {
                    LOGGER.warn("could not re-encode image as PNG for OCR; emitting no content");
                    return;
                }
            }
            metadata.set(Metadata.CONTENT_TYPE, "image/png");
            metadata.set(CONTENT_TYPE_PARSER_OVERRIDE, "image/ocr-png");
            try (InputStream png = TikaInputStream.get(pngFile)) {
                ocrParser.parse(png, handler, metadata, context);
            }
        } finally {
            Files.deleteIfExists(pngFile);
        }
    }

    private static BufferedImage decode(final InputStream stream, final String contentType) throws IOException {
        final byte[] bytes = stream.readAllBytes();
        // Drive readers explicitly: ImageIO.read() relies on SPI format-sniffing that fails on the
        // headerless (PDF-ready) JBIG2 streams Tika emits for embedded images, so select by MIME first.
        BufferedImage image = null;
        if (contentType != null) {
            image = tryReaders(bytes, ImageIO.getImageReadersByMIMEType(contentType));
        }
        // Fall back to format-sniffing when the content type is missing, maps to no reader, or its
        // readers fail to decode — so a claimed type with an unexpected/absent MIME still has a chance.
        if (image == null) {
            try (ImageInputStream iis = ImageIO.createImageInputStream(new ByteArrayInputStream(bytes))) {
                image = tryReaders(bytes, ImageIO.getImageReaders(iis));
            }
        }
        return image;
    }

    private static BufferedImage tryReaders(final byte[] bytes, final Iterator<ImageReader> readers) {
        while (readers.hasNext()) {
            final ImageReader reader = readers.next();
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
