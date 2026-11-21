package org.icij.extract.parser;

import static org.fest.assertions.Assertions.assertThat;
import static org.icij.extract.qrcode.QRCodeImages.canvas;
import static org.icij.extract.qrcode.QRCodeImages.code;
import static org.icij.extract.qrcode.QRCodeImages.write;
import static org.junit.Assert.fail;

import com.google.zxing.BarcodeFormat;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
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
    public void test_values_survive_a_wrapped_parser_that_throws() throws Exception {
        Path file = write(code(BarcodeFormat.QR_CODE, ONE, 300), "png", folder.newFile("one.png").toPath());
        BodyContentHandler handler = new BodyContentHandler(-1);
        Metadata metadata = new Metadata();
        QRCodeParserDecorator parser = new QRCodeParserDecorator(new ThrowingParser());

        try (InputStream input = Files.newInputStream(file)) {
            parser.parse(input, handler, metadata, new ParseContext());
            fail("the wrapped parser failure must still reach the caller");
        } catch (TikaException expected) {
            // the decoded values must outlive the failed parse
        }

        assertThat(metadata.getValues(QRCodeParserDecorator.QR_CODES)).containsOnly(ONE);
        assertThat(handler.toString()).contains("QR code: " + ONE);
    }

    @Test
    public void test_lines_survive_a_wrapped_parser_that_throws_an_error() throws Exception {
        Path file = write(code(BarcodeFormat.QR_CODE, ONE, 300), "png", folder.newFile("one.png").toPath());
        BodyContentHandler handler = new BodyContentHandler(-1);
        QRCodeParserDecorator parser = new QRCodeParserDecorator(new CrashingParser());

        try (InputStream input = Files.newInputStream(file)) {
            parser.parse(input, handler, new Metadata(), new ParseContext());
            fail("the wrapped parser error must still reach the caller");
        } catch (ParserCrash expected) {
            // the lines must be written before the error goes up
        }

        assertThat(handler.toString()).contains("QR code: " + ONE);
    }

    @Test
    public void test_nested_decorator_decodes_once() throws Exception {
        Path file = write(code(BarcodeFormat.QR_CODE, ONE, 300), "png", folder.newFile("one.png").toPath());
        BodyContentHandler handler = new BodyContentHandler(-1);
        Metadata metadata = new Metadata();
        metadata.set(Metadata.CONTENT_TYPE, "image/png");
        Parser nested = new QRCodeParserDecorator(new QRCodeParserDecorator(new ImageParser()));

        try (InputStream input = Files.newInputStream(file)) {
            nested.parse(input, handler, metadata, new ParseContext());
        }

        String[] parts = handler.toString().split("QR code: ", -1);
        int occurrences = parts.length - 1;
        assertThat(occurrences).isEqualTo(1);
        assertThat(metadata.getValues(QRCodeParserDecorator.QR_CODES)).isEqualTo(new String[] {ONE});
    }

    @Test
    public void test_guard_released_after_a_failed_parse() throws Exception {
        Path file = write(code(BarcodeFormat.QR_CODE, ONE, 300), "png", folder.newFile("one.png").toPath());
        ParseContext context = new ParseContext();
        Parser failing = new QRCodeParserDecorator(new ThrowingParser());
        try (InputStream input = Files.newInputStream(file)) {
            failing.parse(input, new BodyContentHandler(-1), new Metadata(), context);
            fail("the wrapped parser failure must still reach the caller");
        } catch (TikaException expected) {
            // the guard must not outlive the failed parse
        }
        Metadata metadata = new Metadata();
        metadata.set(Metadata.CONTENT_TYPE, "image/png");
        Parser working = new QRCodeParserDecorator(new ImageParser());

        try (InputStream input = Files.newInputStream(file)) {
            working.parse(input, new BodyContentHandler(-1), metadata, context);
        }

        assertThat(metadata.getValues(QRCodeParserDecorator.QR_CODES)).isEqualTo(new String[] {ONE});
        assertThat(context.get(QRCodeParserDecorator.class)).isNull();
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

    private static class ThrowingParser extends AbstractParser {
        @Override
        public Set<MediaType> getSupportedTypes(ParseContext context) {
            return Set.of(MediaType.image("png"));
        }

        @Override
        public void parse(InputStream stream, ContentHandler handler, Metadata metadata, ParseContext context)
                throws SAXException, TikaException {
            XHTMLContentHandler xhtml = new XHTMLContentHandler(handler, metadata);
            xhtml.startDocument();
            xhtml.element("p", "partial text");
            throw new TikaException("corrupt image data in the middle of the body");
        }
    }

    private static class CrashingParser extends AbstractParser {
        @Override
        public Set<MediaType> getSupportedTypes(ParseContext context) {
            return Set.of(MediaType.image("png"));
        }

        @Override
        public void parse(InputStream stream, ContentHandler handler, Metadata metadata, ParseContext context)
                throws SAXException {
            XHTMLContentHandler xhtml = new XHTMLContentHandler(handler, metadata);
            xhtml.startDocument();
            xhtml.element("p", "partial text");
            throw new ParserCrash();
        }
    }

    private static class ParserCrash extends Error {
    }
}
