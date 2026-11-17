package org.icij.extract.parser;

import static org.fest.assertions.Assertions.assertThat;
import static org.icij.extract.qrcode.QRCodeImages.canvas;
import static org.icij.extract.qrcode.QRCodeImages.code;
import static org.icij.extract.qrcode.QRCodeImages.write;

import com.google.zxing.BarcodeFormat;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.image.ImageParser;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.ToXMLContentHandler;
import org.icij.spewer.FieldNames;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.xml.sax.ContentHandler;

public class QRCodeParserDecoratorTest {
    private static final String ONE = "https://example.org/qr/one";
    private static final String TWO = "https://example.org/qr/two";

    @Rule public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void test_adds_one_line_and_one_metadata_value_per_code() throws Exception {
        BufferedImage image = canvas(900, 500);
        Graphics2D graphics = image.createGraphics();
        graphics.drawImage(code(BarcodeFormat.QR_CODE, ONE, 400), 50, 50, null);
        graphics.drawImage(code(BarcodeFormat.QR_CODE, TWO, 400), 470, 50, null);
        graphics.dispose();
        Path file = write(image, "png", folder.newFile("two.png").toPath());
        BodyContentHandler handler = new BodyContentHandler(-1);
        Metadata metadata = new Metadata();

        parse(file, handler, metadata);

        assertThat(handler.toString()).contains("QR code: " + ONE + "\nQR code: " + TWO + "\n");
        assertThat(metadata.getValues(QRCodeParserDecorator.QR_CODES)).containsOnly(ONE, TWO);
        assertThat(metadata.getValues(QRCodeParserDecorator.QR_CODES)[0]).isEqualTo(ONE);
    }

    @Test
    public void test_image_without_code_is_untouched() throws Exception {
        Path file = write(canvas(400, 400), "png", folder.newFile("blank.png").toPath());
        BodyContentHandler handler = new BodyContentHandler(-1);
        Metadata metadata = new Metadata();

        parse(file, handler, metadata);

        assertThat(handler.toString()).doesNotContain("QR code");
        assertThat(metadata.get(QRCodeParserDecorator.QR_CODES)).isNull();
    }

    @Test
    public void test_html_output_keeps_the_line_inside_body() throws Exception {
        Path file = write(code(BarcodeFormat.QR_CODE, ONE, 300), "png", folder.newFile("one.png").toPath());
        ToXMLContentHandler handler = new ToXMLContentHandler();

        parse(file, handler, new Metadata());

        String html = handler.toString();
        String paragraph = "<p>QR code: " + ONE + "</p>";
        assertThat(html).contains(paragraph);
        assertThat(html.indexOf(paragraph)).isLessThan(html.indexOf("</body>"));
    }

    @Test
    public void test_spewed_field_name() {
        assertThat(new FieldNames().forMetadata(QRCodeParserDecorator.QR_CODES.getName())).isEqualTo("tika_metadata_qr_codes");
    }

    private static void parse(Path file, ContentHandler handler, Metadata metadata) throws Exception {
        metadata.set(Metadata.CONTENT_TYPE, "image/png");
        QRCodeParserDecorator parser = new QRCodeParserDecorator(new ImageParser());
        try (InputStream input = Files.newInputStream(file)) {
            parser.parse(input, handler, metadata, new ParseContext());
        }
    }
}
