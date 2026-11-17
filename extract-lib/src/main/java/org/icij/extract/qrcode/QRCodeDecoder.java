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
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    private static final Map<DecodeHintType, Object> HINTS = hints();

    private QRCodeDecoder() {}

    /**
     * @return the decoded values in reading order, deduplicated, or an empty list when the file
     * holds no code or cannot be read as an image
     */
    public static List<String> decode(Path image) {
        try {
            BufferedImage gray = readGray(image);
            if (gray == null) {
                return List.of();
            }
            return decode(gray);
        } catch (IOException | RuntimeException e) {
            logger.debug("cannot decode codes in {}", image, e);
            return List.of();
        }
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
            String text = result.getText();
            if (text != null && !text.isBlank()) {
                values.add(text);
            }
        }
        return List.copyOf(values);
    }

    private static BufferedImage readGray(Path image) throws IOException {
        try (ImageInputStream input = ImageIO.createImageInputStream(image.toFile())) {
            if (input == null) {
                return null;
            }
            Iterator<ImageReader> readers = ImageIO.getImageReaders(input);
            if (!readers.hasNext()) {
                return null;
            }
            ImageReader reader = readers.next();
            try {
                reader.setInput(input, true, true);
                long pixels = (long) reader.getWidth(0) * reader.getHeight(0);
                int factor = (int) Math.max(1, Math.ceil(Math.sqrt(pixels / (double) MAX_PIXELS)));
                ImageReadParam param = reader.getDefaultReadParam();
                param.setSourceSubsampling(factor, factor, 0, 0);
                BufferedImage read = reader.read(0, param);
                return toGrayOnWhite(read);
            } finally {
                reader.dispose();
            }
        }
    }

    private static BufferedImage toGrayOnWhite(BufferedImage image) {
        int width = image.getWidth();
        int height = image.getHeight();
        BufferedImage gray = new BufferedImage(width, height, BufferedImage.TYPE_BYTE_GRAY);
        Graphics2D graphics = gray.createGraphics();
        graphics.setColor(Color.WHITE);
        graphics.fillRect(0, 0, width, height);
        graphics.drawImage(image, 0, 0, null);
        graphics.dispose();
        return gray;
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
