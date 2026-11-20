package org.icij.extract.qrcode;

import com.google.zxing.BarcodeFormat;
import com.google.zxing.BinaryBitmap;
import com.google.zxing.DecodeHintType;
import com.google.zxing.LuminanceSource;
import com.google.zxing.MultiFormatReader;
import com.google.zxing.NotFoundException;
import com.google.zxing.PlanarYUVLuminanceSource;
import com.google.zxing.Result;
import com.google.zxing.ResultPoint;
import com.google.zxing.common.HybridBinarizer;
import com.google.zxing.multi.GenericMultipleBarcodeReader;
import java.awt.AWTError;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import javax.imageio.ImageIO;
import javax.imageio.ImageReadParam;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decodes the 2D codes (QR code, Data Matrix, Aztec, PDF417) found in an image file.
 */
public final class QRCodeDecoder {
    private static final Logger logger = LoggerFactory.getLogger(QRCodeDecoder.class);
    private static final int MAX_PIXELS = 16_000_000;
    // The smallest QR code is 21 modules wide, so a smaller image cannot hold a readable one.
    private static final int MIN_SIDE_PIXELS = 21;
    private static final Pattern CONTROL_CHARACTER = Pattern.compile("\\p{Cntrl}");
    private static final Map<DecodeHintType, Object> HINTS = hints();

    private QRCodeDecoder() {}

    /**
     * @return the decoded values in page order, then reading order within a page, deduplicated; a page
     * that fails to read keeps the values of earlier pages; an empty list when the file holds no code or
     * cannot be read as an image
     */
    public static List<String> decode(Path image) {
        Set<String> values = new LinkedHashSet<>();
        try {
            decodeFrames(image, values);
        } catch (IOException | RuntimeException | LinkageError | StackOverflowError | AWTError e) {
            // Optional step: an ImageIO plugin failing with an Error (jai-imageio, levigo, a malformed
            // JPX or JBIG2) must not abort the image parse and lose its OCR text.
            logger.debug("cannot decode codes in {}", image, e);
        }
        return List.copyOf(values);
    }

    private static List<String> decode(BufferedImage gray) {
        int width = gray.getWidth();
        int height = gray.getHeight();
        byte[] pixels = ((DataBufferByte) gray.getRaster().getDataBuffer()).getData();
        LuminanceSource source = new PlanarYUVLuminanceSource(pixels, width, height, 0, 0, width, height, false);
        BinaryBitmap bitmap = new BinaryBitmap(new HybridBinarizer(source));
        GenericMultipleBarcodeReader reader = new GenericMultipleBarcodeReader(new MultiFormatReader());
        Result[] results;
        try {
            results = reader.decodeMultiple(bitmap, HINTS);
        } catch (NotFoundException e) {
            return List.of();
        }
        List<Result> sorted = new ArrayList<>(Arrays.asList(results));
        Comparator<Result> byTop = Comparator.comparingDouble(QRCodeDecoder::minY);
        sorted.sort(byTop.thenComparingDouble(QRCodeDecoder::minX));
        Set<String> values = new LinkedHashSet<>();
        for (Result result : sorted) {
            String text = withoutControlCharacters(result.getText());
            if (!text.isEmpty()) {
                values.add(text);
            }
        }
        return List.copyOf(values);
    }

    // GS1 separators and byte-mode codes carry control characters that are invalid in the XHTML output
    // and in the index, and a newline would break the one-line-per-code layout.
    private static String withoutControlCharacters(String text) {
        if (text == null) {
            return "";
        }
        String spaced = CONTROL_CHARACTER.matcher(text).replaceAll(" ");
        return spaced.strip();
    }

    private static void decodeFrames(Path image, Set<String> values) throws IOException {
        try (ImageInputStream input = ImageIO.createImageInputStream(image.toFile())) {
            if (input == null) {
                return;
            }
            Iterator<ImageReader> readers = ImageIO.getImageReaders(input);
            if (!readers.hasNext()) {
                return;
            }
            ImageReader reader = readers.next();
            try {
                reader.setInput(input, true, true);
                Set<Long> visited = new HashSet<>();
                Thread thread = Thread.currentThread();
                for (int index = 0; !thread.isInterrupted() && hasFrame(reader, input, index, visited); index++) {
                    values.addAll(decodeFrame(reader, index));
                }
            } finally {
                reader.dispose();
            }
        }
    }

    private static boolean hasFrame(ImageReader reader, ImageInputStream input, int index, Set<Long> visited) throws IOException {
        long before = input.getStreamPosition();
        try {
            reader.getWidth(index);
        } catch (IndexOutOfBoundsException e) {
            return false;
        }
        long after = input.getStreamPosition();
        // A reader that cached its pages up front never moves the stream: only a move can reveal a page
        // chain looping back.
        if (after == before) {
            return true;
        }
        return visited.add(after);
    }

    private static List<String> decodeFrame(ImageReader reader, int index) throws IOException {
        int width = reader.getWidth(index);
        int height = reader.getHeight(index);
        if (isTooSmallForACode(width, height)) {
            return List.of();
        }
        long pixels = (long) width * height;
        int factor = (int) Math.max(1, Math.ceil(Math.sqrt(pixels / (double) MAX_PIXELS)));
        ImageReadParam param = reader.getDefaultReadParam();
        param.setSourceSubsampling(factor, factor, 0, 0);
        BufferedImage read = reader.read(index, param);
        return decode(toGrayOnWhite(read));
    }

    // Skips logos and bullets before the costly full decode: PDF inline-image extraction can yield
    // thousands of them per document.
    private static boolean isTooSmallForACode(int width, int height) {
        return width < MIN_SIDE_PIXELS || height < MIN_SIDE_PIXELS;
    }

    // Pixel by pixel rather than Graphics2D: drawing starts AWT, which throws AWTError (not caught as
    // a RuntimeException) when DISPLAY points at an unreachable X server.
    private static BufferedImage toGrayOnWhite(BufferedImage image) {
        int width = image.getWidth();
        int height = image.getHeight();
        BufferedImage gray = new BufferedImage(width, height, BufferedImage.TYPE_BYTE_GRAY);
        byte[] pixels = ((DataBufferByte) gray.getRaster().getDataBuffer()).getData();
        int[] row = new int[width];
        for (int y = 0; y < height; y++) {
            image.getRGB(0, y, width, 1, row, 0, width);
            for (int x = 0; x < width; x++) {
                pixels[y * width + x] = (byte) luminanceOnWhite(row[x]);
            }
        }
        return gray;
    }

    private static int luminanceOnWhite(int argb) {
        int alpha = (argb >>> 24) & 0xFF;
        int red = onWhite((argb >> 16) & 0xFF, alpha);
        int green = onWhite((argb >> 8) & 0xFF, alpha);
        int blue = onWhite(argb & 0xFF, alpha);
        return (299 * red + 587 * green + 114 * blue) / 1000;
    }

    private static int onWhite(int channel, int alpha) {
        return (channel * alpha + 255 * (255 - alpha)) / 255;
    }

    private static double minY(Result result) {
        return Arrays.stream(result.getResultPoints()).mapToDouble(ResultPoint::getY).min().orElse(0);
    }

    private static double minX(Result result) {
        return Arrays.stream(result.getResultPoints()).mapToDouble(ResultPoint::getX).min().orElse(0);
    }

    private static Map<DecodeHintType, Object> hints() {
        Map<DecodeHintType, Object> hints = new EnumMap<>(DecodeHintType.class);
        List<BarcodeFormat> formats = List.of(BarcodeFormat.QR_CODE, BarcodeFormat.DATA_MATRIX, BarcodeFormat.AZTEC, BarcodeFormat.PDF_417);
        hints.put(DecodeHintType.POSSIBLE_FORMATS, formats);
        hints.put(DecodeHintType.TRY_HARDER, Boolean.TRUE);
        return hints;
    }
}
