package org.icij.extract.qrcode;

import com.google.zxing.BarcodeFormat;
import com.google.zxing.MultiFormatWriter;
import com.google.zxing.WriterException;
import com.google.zxing.common.BitMatrix;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageWriter;
import javax.imageio.stream.ImageOutputStream;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDDocumentCatalog;
import org.apache.pdfbox.pdmodel.PDDocumentNameDictionary;
import org.apache.pdfbox.pdmodel.PDEmbeddedFilesNameTreeNode;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageContentStream;
import org.apache.pdfbox.pdmodel.common.filespecification.PDComplexFileSpecification;
import org.apache.pdfbox.pdmodel.common.filespecification.PDEmbeddedFile;
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

    public static Path writeSequence(List<BufferedImage> pages, String format, Path file) throws IOException {
        Iterator<ImageWriter> writers = ImageIO.getImageWritersByFormatName(format);
        ImageWriter writer = writers.next();
        try (ImageOutputStream output = ImageIO.createImageOutputStream(file.toFile())) {
            writer.setOutput(output);
            writer.prepareWriteSequence(null);
            for (BufferedImage page : pages) {
                IIOImage frame = new IIOImage(page, null, null);
                writer.writeToSequence(frame, null);
            }
            writer.endWriteSequence();
        } finally {
            writer.dispose();
        }
        return file;
    }

    public static Path loopLastPageToFirst(Path tiff) throws IOException {
        byte[] bytes = Files.readAllBytes(tiff);
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        ByteOrder order = bytes[0] == 'I' ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
        buffer.order(order);
        int first = buffer.getInt(4);
        int nextField = nextIfdField(buffer, first);
        int next = buffer.getInt(nextField);
        while (next != 0) {
            nextField = nextIfdField(buffer, next);
            next = buffer.getInt(nextField);
        }
        buffer.putInt(nextField, first);
        Files.write(tiff, bytes);
        return tiff;
    }

    private static int nextIfdField(ByteBuffer buffer, int ifd) {
        int entries = Short.toUnsignedInt(buffer.getShort(ifd));
        return ifd + 2 + entries * 12;
    }

    public static Path pdfWithImage(BufferedImage image, Path file) throws IOException {
        return pdfWithImageOnEveryPage(image, 1, file);
    }

    public static Path pdfWithImageOnEveryPage(BufferedImage image, int pageCount, Path file) throws IOException {
        try (PDDocument document = new PDDocument()) {
            PDImageXObject pdImage = LosslessFactory.createFromImage(document, image);
            for (int pageIndex = 0; pageIndex < pageCount; pageIndex++) {
                PDPage page = new PDPage();
                document.addPage(page);
                try (PDPageContentStream content = new PDPageContentStream(document, page)) {
                    content.drawImage(pdImage, 50, 300, 300, 300);
                }
            }
            document.save(file.toFile());
        }
        return file;
    }

    public static Path pdfWithImageAndAttachment(BufferedImage pageImage, Path attachment, Path file) throws IOException {
        try (PDDocument document = new PDDocument()) {
            PDImageXObject pdImage = LosslessFactory.createFromImage(document, pageImage);
            PDPage page = new PDPage();
            document.addPage(page);
            try (PDPageContentStream content = new PDPageContentStream(document, page)) {
                content.drawImage(pdImage, 50, 300, 300, 300);
            }
            Path fileName = attachment.getFileName();
            String name = fileName.toString();
            byte[] bytes = Files.readAllBytes(attachment);
            ByteArrayInputStream attachmentStream = new ByteArrayInputStream(bytes);
            PDEmbeddedFile embedded = new PDEmbeddedFile(document, attachmentStream);
            embedded.setSubtype("image/png");
            PDComplexFileSpecification spec = new PDComplexFileSpecification();
            spec.setFile(name);
            spec.setEmbeddedFile(embedded);
            PDEmbeddedFilesNameTreeNode tree = new PDEmbeddedFilesNameTreeNode();
            Map<String, PDComplexFileSpecification> entries = Map.of(name, spec);
            tree.setNames(entries);
            PDDocumentCatalog catalog = document.getDocumentCatalog();
            PDDocumentNameDictionary names = new PDDocumentNameDictionary(catalog);
            names.setEmbeddedFiles(tree);
            catalog.setNames(names);
            document.save(file.toFile());
        }
        return file;
    }
}
