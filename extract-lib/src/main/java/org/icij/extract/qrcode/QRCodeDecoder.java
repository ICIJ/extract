package org.icij.extract.qrcode;

import com.google.zxing.BarcodeFormat;
import com.google.zxing.BinaryBitmap;
import com.google.zxing.DecodeHintType;
import com.google.zxing.LuminanceSource;
import com.google.zxing.MultiFormatReader;
import com.google.zxing.NotFoundException;
import com.google.zxing.Result;
import com.google.zxing.ResultPoint;
import com.google.zxing.client.j2se.BufferedImageLuminanceSource;
import com.google.zxing.common.HybridBinarizer;
import com.google.zxing.multi.GenericMultipleBarcodeReader;
import org.icij.extract.image.ImageIOReaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageReadParam;
import javax.imageio.ImageReader;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Decodes the 2D codes (QR code, Data Matrix, Aztec, PDF417) found on every page of an image file.
 */
public final class QRCodeDecoder {
    private static final Logger logger = LoggerFactory.getLogger(QRCodeDecoder.class);
    private static final int MAX_PIXELS = 16_000_000;
    private static final Map<DecodeHintType, Object> HINTS = hints();

    static final Comparator<Result> READING_ORDER = Comparator.comparingDouble(QRCodeDecoder::minY)
            .thenComparingDouble(QRCodeDecoder::minX);

    private QRCodeDecoder() {}

    /**
     * @param contentType selects the ImageIO readers to try first, may be null
     * @return the decoded values in reading order, page after page, deduplicated, or an empty list when the
     * file holds no code or cannot be read as an image
     */
    public static List<String> decode(Path image, String contentType) {
        try {
            Set<String> values = ImageIOReaders.read(image, contentType, QRCodeDecoder::decodePages);
            if (values == null) {
                return List.of();
            }
            return List.copyOf(values);
        } catch (IOException | RuntimeException e) {
            logger.debug("cannot decode codes in {}", image, e);
            return List.of();
        }
    }

    private static Set<String> decodePages(ImageReader reader) throws IOException {
        Set<String> values = new LinkedHashSet<>();
        int pages = reader.getNumImages(true);
        for (int page = 0; page < pages; page++) {
            BufferedImage image = readSubsampled(reader, page);
            values.addAll(decode(image));
        }
        return values;
    }

    private static BufferedImage readSubsampled(ImageReader reader, int page) throws IOException {
        long pixels = (long) reader.getWidth(page) * reader.getHeight(page);
        double ratio = pixels / (double) MAX_PIXELS;
        double side = Math.ceil(Math.sqrt(ratio));
        int factor = Math.max(1, (int) side);
        ImageReadParam param = reader.getDefaultReadParam();
        param.setSourceSubsampling(factor, factor, 0, 0);
        return reader.read(page, param);
    }

    // BufferedImageLuminanceSource converts pixel by pixel, never through Graphics2D: drawing starts AWT,
    // which throws AWTError (not caught as a RuntimeException) when DISPLAY points at an unreachable X server.
    private static List<String> decode(BufferedImage image) {
        LuminanceSource source = new BufferedImageLuminanceSource(image);
        BinaryBitmap bitmap = new BinaryBitmap(new HybridBinarizer(source));
        GenericMultipleBarcodeReader reader = new GenericMultipleBarcodeReader(new MultiFormatReader());
        Result[] results;
        try {
            results = reader.decodeMultiple(bitmap, HINTS);
        } catch (NotFoundException e) {
            return List.of();
        }
        List<Result> sorted = Arrays.asList(results);
        sorted.sort(READING_ORDER);
        List<String> values = new ArrayList<>();
        for (Result result : sorted) {
            String text = result.getText();
            if (text != null && !text.isBlank()) {
                values.add(text);
            }
        }
        return values;
    }

    private static double minY(Result result) {
        return points(result).mapToDouble(ResultPoint::getY).min().orElse(0);
    }

    private static double minX(Result result) {
        return points(result).mapToDouble(ResultPoint::getX).min().orElse(0);
    }

    // PDF417 results can hold null points when the detector found only part of the code.
    private static Stream<ResultPoint> points(Result result) {
        ResultPoint[] points = result.getResultPoints();
        if (points == null) {
            return Stream.empty();
        }
        return Arrays.stream(points).filter(Objects::nonNull);
    }

    private static Map<DecodeHintType, Object> hints() {
        Map<DecodeHintType, Object> hints = new EnumMap<>(DecodeHintType.class);
        List<BarcodeFormat> formats = List.of(BarcodeFormat.QR_CODE, BarcodeFormat.DATA_MATRIX, BarcodeFormat.AZTEC, BarcodeFormat.PDF_417);
        hints.put(DecodeHintType.POSSIBLE_FORMATS, formats);
        hints.put(DecodeHintType.TRY_HARDER, Boolean.TRUE);
        return hints;
    }
}
