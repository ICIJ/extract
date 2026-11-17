package org.icij.extract.parser;

import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.fail;
import static org.icij.extract.qrcode.QRCodeImages.canvas;
import static org.icij.extract.qrcode.QRCodeImages.code;
import static org.icij.extract.qrcode.QRCodeImages.write;

import com.google.zxing.BarcodeFormat;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.ParserDecorator;
import org.apache.tika.parser.image.ImageParser;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.ToXMLContentHandler;
import org.apache.tika.sax.XHTMLContentHandler;
import org.icij.spewer.FieldNames;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

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

    @Test
    public void test_values_kept_in_metadata_when_the_wrapped_parser_fails() throws Exception {
        Path file = write(code(BarcodeFormat.QR_CODE, ONE, 300), "png", folder.newFile("one.png").toPath());
        Metadata metadata = new Metadata();
        Parser failing = new Parser() {
            @Override
            public Set<MediaType> getSupportedTypes(ParseContext context) {
                return Set.of(MediaType.image("png"));
            }

            @Override
            public void parse(InputStream stream, ContentHandler handler, Metadata metadata, ParseContext context) throws TikaException {
                throw new TikaException("cannot read metadata");
            }
        };

        try {
            parse(new QRCodeParserDecorator(failing), file, new BodyContentHandler(-1), metadata);
            fail("expected a TikaException");
        } catch (TikaException expected) {
            assertThat(metadata.getValues(QRCodeParserDecorator.QR_CODES)).containsOnly(ONE);
        }
    }

    @Test
    public void test_ocr_handoff_decodes_once() throws Exception {
        Path file = write(code(BarcodeFormat.QR_CODE, ONE, 300), "png", folder.newFile("one.png").toPath());
        BodyContentHandler handler = new BodyContentHandler(-1);
        Metadata metadata = new Metadata();
        Parser handoff = new ParserDecorator(new QRCodeParserDecorator(new ImageParser())) {
            @Override
            public void parse(InputStream stream, ContentHandler handler, Metadata metadata, ParseContext context) throws IOException, SAXException, TikaException {
                metadata.set(TikaCoreProperties.CONTENT_TYPE_PARSER_OVERRIDE, "image/ocr-png");
                super.parse(stream, handler, metadata, context);
            }
        };

        parse(new QRCodeParserDecorator(handoff), file, handler, metadata);

        assertThat(handler.toString().split("QR code: ", -1)).hasSize(2);
        assertThat(metadata.getValues(QRCodeParserDecorator.QR_CODES)).isEqualTo(new String[] {ONE});
    }

    @Test
    public void test_image_embedded_in_an_image_is_decoded() throws Exception {
        Path file = write(code(BarcodeFormat.QR_CODE, ONE, 300), "png", folder.newFile("one.png").toPath());
        Metadata embedded = new Metadata();
        Parser withEmbed = new ParserDecorator(new ImageParser()) {
            @Override
            public void parse(InputStream stream, ContentHandler handler, Metadata metadata, ParseContext context) throws IOException, SAXException, TikaException {
                super.parse(stream, handler, metadata, context);
                QRCodeParserDecorator inner = new QRCodeParserDecorator(new ImageParser());
                embedded.set(Metadata.CONTENT_TYPE, "image/png");
                try (InputStream input = Files.newInputStream(file)) {
                    inner.parse(input, new BodyContentHandler(-1), embedded, context);
                }
            }
        };

        parse(new QRCodeParserDecorator(withEmbed), file, new BodyContentHandler(-1), new Metadata());

        assertThat(embedded.getValues(QRCodeParserDecorator.QR_CODES)).containsOnly(ONE);
    }

    @Test
    public void test_line_written_when_the_wrapped_parser_never_closes_the_body() throws Exception {
        Path file = write(code(BarcodeFormat.QR_CODE, ONE, 300), "png", folder.newFile("one.png").toPath());
        BodyContentHandler handler = new BodyContentHandler(-1);
        Parser cached = new Parser() {
            @Override
            public Set<MediaType> getSupportedTypes(ParseContext context) {
                return Set.of(MediaType.image("png"));
            }

            @Override
            public void parse(InputStream stream, ContentHandler handler, Metadata metadata, ParseContext context) throws SAXException {
                XHTMLContentHandler xhtml = new XHTMLContentHandler(handler, metadata);
                xhtml.element("div", "cached text");
            }
        };

        parse(new QRCodeParserDecorator(cached), file, handler, new Metadata());

        assertThat(handler.toString()).contains("cached text");
        assertThat(handler.toString()).contains("QR code: " + ONE);
    }

    @Test
    public void test_line_written_when_the_wrapped_parser_writes_nothing() throws Exception {
        Path file = write(code(BarcodeFormat.QR_CODE, ONE, 300), "png", folder.newFile("one.png").toPath());
        BodyContentHandler handler = new BodyContentHandler(-1);
        Parser silent = new Parser() {
            @Override
            public Set<MediaType> getSupportedTypes(ParseContext context) {
                return Set.of(MediaType.image("png"));
            }

            @Override
            public void parse(InputStream stream, ContentHandler handler, Metadata metadata, ParseContext context) {
            }
        };

        parse(new QRCodeParserDecorator(silent), file, handler, new Metadata());

        assertThat(handler.toString()).contains("QR code: " + ONE);
    }

    @Test
    public void test_add_qr_codes_skips_duplicates_on_both_sides() {
        Metadata from = new Metadata();
        from.add(QRCodeParserDecorator.QR_CODES, ONE);
        from.add(QRCodeParserDecorator.QR_CODES, ONE);
        Metadata to = new Metadata();
        to.add(QRCodeParserDecorator.QR_CODES, TWO);
        to.add(QRCodeParserDecorator.QR_CODES, TWO);

        QRCodeParserDecorator.addQrCodes(from, to);

        assertThat(to.getValues(QRCodeParserDecorator.QR_CODES)).isEqualTo(new String[] {TWO, TWO, ONE});
    }

    private static void parse(Path file, ContentHandler handler, Metadata metadata) throws Exception {
        parse(new QRCodeParserDecorator(new ImageParser()), file, handler, metadata);
    }

    private static void parse(Parser parser, Path file, ContentHandler handler, Metadata metadata) throws Exception {
        metadata.set(Metadata.CONTENT_TYPE, "image/png");
        try (InputStream input = Files.newInputStream(file)) {
            parser.parse(input, handler, metadata, new ParseContext());
        }
    }
}
