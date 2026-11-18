package org.icij.extract.qrcode;

import static org.fest.assertions.Assertions.assertThat;
import static org.icij.extract.qrcode.QRCodeImages.canvas;
import static org.icij.extract.qrcode.QRCodeImages.code;
import static org.icij.extract.qrcode.QRCodeImages.write;

import com.google.zxing.BarcodeFormat;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.geom.AffineTransform;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Locale;
import javax.imageio.ImageReadParam;
import javax.imageio.ImageReader;
import javax.imageio.ImageTypeSpecifier;
import javax.imageio.metadata.IIOMetadata;
import javax.imageio.spi.IIORegistry;
import javax.imageio.spi.ImageReaderSpi;
import javax.imageio.stream.ImageInputStream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class QRCodeDecoderTest {
    private static final String ONE = "https://example.org/qr/one";
    private static final String TWO = "https://example.org/qr/two";
    private static final String THREE = "https://example.org/qr/three";

    @Rule public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void test_image_without_code_returns_empty() throws Exception {
        Path file = write(canvas(400, 400), "png", folder.newFile("blank.png").toPath());

        assertThat(QRCodeDecoder.decode(file)).isEmpty();
    }

    @Test
    public void test_one_qr_code() throws Exception {
        Path file = write(code(BarcodeFormat.QR_CODE, ONE, 300), "png", folder.newFile("one.png").toPath());

        assertThat(QRCodeDecoder.decode(file)).containsExactly(ONE);
    }

    @Test
    public void test_several_codes_in_reading_order() throws Exception {
        BufferedImage image = canvas(1400, 500);
        Graphics2D graphics = image.createGraphics();
        graphics.drawImage(code(BarcodeFormat.QR_CODE, THREE, 400), 950, 50, null);
        graphics.drawImage(code(BarcodeFormat.QR_CODE, ONE, 400), 50, 50, null);
        graphics.drawImage(code(BarcodeFormat.QR_CODE, TWO, 400), 500, 50, null);
        graphics.dispose();
        Path file = write(image, "png", folder.newFile("several.png").toPath());

        assertThat(QRCodeDecoder.decode(file)).containsExactly(ONE, TWO, THREE);
    }

    @Test
    public void test_duplicate_codes_are_returned_once() throws Exception {
        BufferedImage image = canvas(900, 500);
        BufferedImage qr = code(BarcodeFormat.QR_CODE, ONE, 400);
        Graphics2D graphics = image.createGraphics();
        graphics.drawImage(qr, 50, 50, null);
        graphics.drawImage(qr, 470, 50, null);
        graphics.dispose();
        Path file = write(image, "png", folder.newFile("twice.png").toPath());

        assertThat(QRCodeDecoder.decode(file)).containsExactly(ONE);
    }

    @Test
    public void test_data_matrix_aztec_and_pdf417() throws Exception {
        Path dataMatrix = write(code(BarcodeFormat.DATA_MATRIX, "DM-12345", 300), "png", folder.newFile("dm.png").toPath());
        Path aztec = write(code(BarcodeFormat.AZTEC, "AZTEC-42", 300), "png", folder.newFile("aztec.png").toPath());
        Path pdf417 = write(code(BarcodeFormat.PDF_417, "PDF417-7", 600), "png", folder.newFile("pdf417.png").toPath());

        assertThat(QRCodeDecoder.decode(dataMatrix)).containsExactly("DM-12345");
        assertThat(QRCodeDecoder.decode(aztec)).containsExactly("AZTEC-42");
        assertThat(QRCodeDecoder.decode(pdf417)).containsExactly("PDF417-7");
    }

    @Test
    public void test_rotated_code() throws Exception {
        BufferedImage qr = code(BarcodeFormat.QR_CODE, ONE, 300);
        BufferedImage rotated = canvas(qr.getHeight(), qr.getWidth());
        Graphics2D graphics = rotated.createGraphics();
        AffineTransform rotation = AffineTransform.getQuadrantRotateInstance(1, qr.getWidth() / 2.0, qr.getHeight() / 2.0);
        graphics.drawImage(qr, rotation, null);
        graphics.dispose();
        Path file = write(rotated, "png", folder.newFile("rotated.png").toPath());

        assertThat(QRCodeDecoder.decode(file)).containsExactly(ONE);
    }

    @Test
    public void test_transparent_background() throws Exception {
        BufferedImage qr = code(BarcodeFormat.QR_CODE, ONE, 300);
        BufferedImage transparent = new BufferedImage(qr.getWidth(), qr.getHeight(), BufferedImage.TYPE_INT_ARGB);
        int black = Color.BLACK.getRGB();
        for (int y = 0; y < qr.getHeight(); y++) {
            for (int x = 0; x < qr.getWidth(); x++) {
                if (qr.getRGB(x, y) == black) {
                    transparent.setRGB(x, y, black);
                }
            }
        }
        Path file = write(transparent, "png", folder.newFile("transparent.png").toPath());

        assertThat(QRCodeDecoder.decode(file)).containsExactly(ONE);
    }

    @Test
    public void test_large_scan_is_subsampled() throws Exception {
        BufferedImage image = canvas(6000, 6000);
        Graphics2D graphics = image.createGraphics();
        graphics.drawImage(code(BarcodeFormat.QR_CODE, ONE, 1200), 2400, 2400, null);
        graphics.dispose();
        Path file = write(image, "png", folder.newFile("large.png").toPath());

        assertThat(QRCodeDecoder.decode(file)).containsExactly(ONE);
    }

    @Test
    public void test_non_image_returns_empty() throws Exception {
        Path file = folder.newFile("hello.txt").toPath();
        Files.writeString(file, "hello", StandardCharsets.UTF_8);

        assertThat(QRCodeDecoder.decode(file)).isEmpty();
    }

    @Test
    public void test_truncated_image_returns_empty() throws Exception {
        Path full = write(code(BarcodeFormat.QR_CODE, ONE, 300), "png", folder.newFile("full.png").toPath());
        byte[] bytes = Files.readAllBytes(full);
        byte[] head = Arrays.copyOf(bytes, 100);
        Path truncated = folder.newFile("truncated.png").toPath();
        Files.write(truncated, head);

        assertThat(QRCodeDecoder.decode(truncated)).isEmpty();
    }

    // Built with setRGB only, so the test itself never touches AWT graphics: run with an unreachable
    // DISPLAY it proves decoding does not either.
    @Test
    public void test_decodes_without_awt_graphics() throws Exception {
        String value = "https://example.org/qr/headless";
        com.google.zxing.common.BitMatrix matrix = new com.google.zxing.MultiFormatWriter()
                .encode(value, BarcodeFormat.QR_CODE, 300, 300);
        BufferedImage image = new BufferedImage(matrix.getWidth(), matrix.getHeight(), BufferedImage.TYPE_INT_ARGB);
        for (int y = 0; y < matrix.getHeight(); y++) {
            for (int x = 0; x < matrix.getWidth(); x++) {
                int pixel = matrix.get(x, y) ? 0xFF000000 : 0x00000000;
                image.setRGB(x, y, pixel);
            }
        }
        Path file = write(image, "png", folder.newFile("headless.png").toPath());

        assertThat(QRCodeDecoder.decode(file)).containsExactly(value);
    }

    @Test
    public void test_error_from_an_image_reader_returns_empty() throws Exception {
        Path file = folder.newFile("broken.boom").toPath();
        Files.write(file, BrokenReaderSpi.MAGIC);
        BrokenReaderSpi spi = new BrokenReaderSpi();
        IIORegistry.getDefaultInstance().registerServiceProvider(spi);
        try {
            assertThat(QRCodeDecoder.decode(file)).isEmpty();
        } finally {
            IIORegistry.getDefaultInstance().deregisterServiceProvider(spi);
        }
    }

    // Stands in for a jai-imageio or levigo plugin that fails with an Error rather than an exception.
    private static class BrokenReaderSpi extends ImageReaderSpi {
        static final byte[] MAGIC = "BOOM".getBytes(StandardCharsets.US_ASCII);

        BrokenReaderSpi() {
            super("test", "1", new String[] {"boom"}, new String[] {"boom"}, new String[] {"image/x-boom"},
                    BrokenReader.class.getName(), new Class<?>[] {ImageInputStream.class}, null,
                    false, null, null, null, null, false, null, null, null, null);
        }

        @Override
        public boolean canDecodeInput(Object source) throws IOException {
            ImageInputStream input = (ImageInputStream) source;
            byte[] head = new byte[MAGIC.length];
            input.mark();
            input.readFully(head);
            input.reset();
            return Arrays.equals(head, MAGIC);
        }

        @Override
        public ImageReader createReaderInstance(Object extension) {
            return new BrokenReader(this);
        }

        @Override
        public String getDescription(Locale locale) {
            return "reader failing with a LinkageError";
        }
    }

    private static class BrokenReader extends ImageReader {
        BrokenReader(ImageReaderSpi spi) {
            super(spi);
        }

        @Override
        public int getNumImages(boolean allowSearch) {
            return 1;
        }

        @Override
        public int getWidth(int imageIndex) {
            throw new LinkageError("native codec missing");
        }

        @Override
        public int getHeight(int imageIndex) {
            throw new LinkageError("native codec missing");
        }

        @Override
        public Iterator<ImageTypeSpecifier> getImageTypes(int imageIndex) {
            return Collections.emptyIterator();
        }

        @Override
        public IIOMetadata getStreamMetadata() {
            return null;
        }

        @Override
        public IIOMetadata getImageMetadata(int imageIndex) {
            return null;
        }

        @Override
        public BufferedImage read(int imageIndex, ImageReadParam param) {
            throw new LinkageError("native codec missing");
        }
    }
}
