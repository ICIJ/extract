package org.icij.extract.image;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;

/**
 * Reads an image file with the first ImageIO reader that succeeds. Readers registered for the content type
 * come first: format sniffing fails on the headerless (PDF-ready) JBIG2 streams Tika emits for embedded
 * images. Sniffed readers come next, so a missing or wrong content type, or a reader that rejects the
 * image (a CMYK JPEG), still gets a chance.
 */
public final class ImageIOReaders {
    private static final Logger LOGGER = LoggerFactory.getLogger(ImageIOReaders.class);

    @FunctionalInterface
    public interface Read<T> {
        T apply(ImageReader reader) throws IOException;
    }

    private ImageIOReaders() {}

    /** @return what {@code read} returned for the first reader that did not throw, or null when none did */
    public static <T> T read(final Path image, final String contentType, final Read<T> read) throws IOException {
        T result = null;
        if (contentType != null) {
            result = tryReaders(image, ImageIO.getImageReadersByMIMEType(contentType), read);
        }
        if (result != null) {
            return result;
        }
        try (ImageInputStream input = ImageIO.createImageInputStream(image.toFile())) {
            return tryReaders(image, ImageIO.getImageReaders(input), read);
        }
    }

    private static <T> T tryReaders(final Path image, final Iterator<ImageReader> readers, final Read<T> read) {
        while (readers.hasNext()) {
            final ImageReader reader = readers.next();
            try (ImageInputStream input = ImageIO.createImageInputStream(image.toFile())) {
                reader.setInput(input);
                return read.apply(reader);
            } catch (final Exception e) {
                LOGGER.debug("reader {} failed to decode image: {}", reader.getClass().getName(), e.toString());
            } finally {
                reader.dispose();
            }
        }
        return null;
    }
}
