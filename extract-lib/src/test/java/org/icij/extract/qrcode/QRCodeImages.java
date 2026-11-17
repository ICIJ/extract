package org.icij.extract.qrcode;

import com.google.zxing.BarcodeFormat;
import com.google.zxing.MultiFormatWriter;
import com.google.zxing.WriterException;
import com.google.zxing.common.BitMatrix;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.nio.file.Path;
import javax.imageio.ImageIO;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageContentStream;
import org.apache.pdfbox.pdmodel.graphics.image.LosslessFactory;
import org.apache.pdfbox.pdmodel.graphics.image.PDImageXObject;

public final class QRCodeImages {
    private QRCodeImages() {}

    public static BufferedImage code(BarcodeFormat format, String text, int size) throws WriterException {
        int height = format == BarcodeFormat.PDF_417 ? size / 3 : size;
        BitMatrix matrix = new MultiFormatWriter().encode(text, format, size, height);
        BufferedImage image = canvas(matrix.getWidth(), matrix.getHeight());
        for (int y = 0; y < matrix.getHeight(); y++) {
            for (int x = 0; x < matrix.getWidth(); x++) {
                if (matrix.get(x, y)) {
                    image.setRGB(x, y, Color.BLACK.getRGB());
                }
            }
        }
        return image;
    }

    public static BufferedImage canvas(int width, int height) {
        BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_BYTE_GRAY);
        Graphics2D graphics = image.createGraphics();
        graphics.setColor(Color.WHITE);
        graphics.fillRect(0, 0, width, height);
        graphics.dispose();
        return image;
    }

    public static Path write(BufferedImage image, String format, Path file) throws IOException {
        ImageIO.write(image, format, file.toFile());
        return file;
    }

    public static Path pdfWithImage(BufferedImage image, Path file) throws IOException {
        try (PDDocument document = new PDDocument()) {
            PDPage page = new PDPage();
            document.addPage(page);
            PDImageXObject pdImage = LosslessFactory.createFromImage(document, image);
            try (PDPageContentStream content = new PDPageContentStream(document, page)) {
                content.drawImage(pdImage, 50, 300, 300, 300);
            }
            document.save(file.toFile());
        }
        return file;
    }
}
