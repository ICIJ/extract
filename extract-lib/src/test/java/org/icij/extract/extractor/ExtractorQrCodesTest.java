package org.icij.extract.extractor;

import static org.fest.assertions.Assertions.assertThat;
import static org.icij.extract.parser.QRCodeParserDecorator.QR_CODES;
import static org.icij.extract.qrcode.QRCodeImages.canvas;
import static org.icij.extract.qrcode.QRCodeImages.code;
import static org.icij.extract.qrcode.QRCodeImages.pdfWithImage;
import static org.icij.extract.qrcode.QRCodeImages.pdfWithImageOnEveryPage;
import static org.icij.extract.qrcode.QRCodeImages.write;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.google.zxing.BarcodeFormat;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.icij.extract.document.EmbeddedTikaDocument;
import org.icij.extract.document.TikaDocument;
import org.icij.spewer.FieldNames;
import org.icij.spewer.PrintStreamSpewer;
import org.icij.spewer.Spewer;
import org.icij.task.Options;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.LoggerFactory;

public class ExtractorQrCodesTest {
    private static final String VALUE = "https://example.org/qr/one";
    private static final String LINE = "QR code: " + VALUE;

    @Rule public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void test_inline_images_kept_when_ocr_off_and_qr_codes_on() {
        Extractor extractor = new Extractor(Options.from(Map.of("ocr", "false", "qrCodes", "true")));

        assertThat(extractor.isExtractInlineImages()).isTrue();
    }

    @Test
    public void test_inline_images_off_when_ocr_off_and_qr_codes_off() {
        Extractor extractor = new Extractor(Options.from(Map.of("ocr", "false")));

        assertThat(extractor.isExtractInlineImages()).isFalse();
    }

    @Test
    public void test_rendering_strategy_still_disables_inline_images() {
        Extractor extractor = new Extractor(Options.from(Map.of("qrCodes", "true", "ocrStrategy", "AUTO")));

        assertThat(extractor.isExtractInlineImages()).isFalse();
    }

    @Test
    public void test_rendering_strategy_warns_that_pdf_codes_are_not_read() {
        Logger extractorLogger = (Logger) LoggerFactory.getLogger(Extractor.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        extractorLogger.addAppender(appender);
        try {
            new Extractor(Options.from(Map.of("qrCodes", "true", "ocrStrategy", "AUTO")));
        } finally {
            extractorLogger.detachAppender(appender);
        }

        List<String> warnings = appender.list.stream()
                .filter(event -> event.getLevel() == Level.WARN)
                .map(ILoggingEvent::getFormattedMessage)
                .filter(message -> message.contains("qrCodes"))
                .toList();
        assertThat(warnings).isNotEmpty();
    }

    @Test
    public void test_ocr_off_forces_no_ocr_so_qr_codes_keep_inline_images() {
        Options<String> options = Options.from(Map.of("ocr", "false", "qrCodes", "true", "ocrStrategy", "AUTO"));
        Extractor extractor = new Extractor(options);

        assertThat(extractor.isExtractInlineImages()).isTrue();
    }

    @Test
    public void test_standalone_png_ocr_on() throws Exception {
        Path file = qrImage("png");
        Extractor extractor = new Extractor(Options.from(Map.of("qrCodes", "true")));

        TikaDocument document = extractor.extract(file);
        String text = read(document);

        assertThat(text).contains(LINE);
        assertThat(document.getMetadata().getValues(QR_CODES)).containsOnly(VALUE);
    }

    @Test
    public void test_every_covered_format() throws Exception {
        for (String format : List.of("png", "jpg", "gif", "bmp", "tif")) {
            Path file = qrImage(format);
            Extractor extractor = new Extractor(Options.from(Map.of("ocr", "false", "qrCodes", "true")));

            TikaDocument document = extractor.extract(file);
            String text = read(document);

            assertThat(text).as(format).contains(LINE);
            assertThat(document.getMetadata().getValues(QR_CODES)).as(format).containsOnly(VALUE);
        }
    }

    // WBMP takes the same transcoding route as JBIG2 scans when OCR is on, and is easier to write.
    @Test
    public void test_transcoded_image_ocr_on() throws Exception {
        Extractor extractor = new Extractor(Options.from(Map.of("qrCodes", "true")));

        TikaDocument document = extractor.extract(qrWbmp());
        String text = read(document);

        assertThat(text).contains(LINE);
        assertThat(document.getMetadata().getValues(QR_CODES)).containsOnly(VALUE);
    }

    @Test
    public void test_transcoded_image_ocr_on_with_ocr_cache() throws Exception {
        String cache = folder.newFolder("ocr-cache").toString();
        Extractor extractor = new Extractor(Options.from(Map.of("qrCodes", "true", "ocrCache", cache)));

        TikaDocument document = extractor.extract(qrWbmp());
        read(document);

        assertThat(document.getMetadata().getValues(QR_CODES)).containsOnly(VALUE);
    }

    @Test
    public void test_transcoded_image_ocr_cache_written_with_qr_codes_on() throws Exception {
        Path cache = folder.newFolder("ocr-cache").toPath();
        Extractor extractor = new Extractor(Options.from(Map.of("qrCodes", "true", "ocrCache", cache.toString())));

        read(extractor.extract(qrWbmp()));

        try (Stream<Path> entries = Files.list(cache)) {
            assertThat(entries.toList()).isNotEmpty();
        }
    }

    @Test
    public void test_image_without_code() throws Exception {
        Path file = write(canvas(400, 400), "png", folder.newFile("blank.png").toPath());
        Extractor extractor = new Extractor(Options.from(Map.of("ocr", "false", "qrCodes", "true")));

        TikaDocument document = extractor.extract(file);
        String text = read(document);

        assertThat(text).doesNotContain("QR code");
        assertThat(document.getMetadata().get(QR_CODES)).isNull();
    }

    @Test
    public void test_option_off_by_default() throws Exception {
        Path file = qrImage("png");
        Extractor extractor = new Extractor(Options.from(Map.of("ocr", "false")));

        TikaDocument document = extractor.extract(file);
        String text = read(document);

        assertThat(text).doesNotContain("QR code");
        assertThat(document.getMetadata().get(QR_CODES)).isNull();
    }

    @Test
    public void test_pdf_inline_image_ocr_off() throws Exception {
        Extractor extractor = new Extractor(Options.from(Map.of("ocr", "false", "qrCodes", "true")));

        assertThat(treeText(extractor, qrPdf())).contains(LINE);
    }

    @Test
    public void test_pdf_inline_image_values_reach_the_pdf_metadata() throws Exception {
        Extractor extractor = new Extractor(Options.from(Map.of("ocr", "false", "qrCodes", "true")));

        TikaDocument root = extractor.extract(qrPdf());
        spew(root);

        assertThat(root.getEmbeds()).isEmpty();
        assertThat(root.getMetadata().getValues(QR_CODES)).containsOnly(VALUE);
    }

    @Test
    public void test_pdf_inline_image_values_reach_the_pdf_metadata_in_concatenate_mode() throws Exception {
        Options<String> options = Options.from(Map.of("ocr", "false", "qrCodes", "true", "embedHandling", "concatenate"));
        Extractor extractor = new Extractor(options);

        TikaDocument root = extractor.extract(qrPdf());
        spew(root);

        assertThat(root.getMetadata().getValues(QR_CODES)).containsOnly(VALUE);
    }

    @Test
    public void test_code_repeated_on_every_page_is_kept_once() throws Exception {
        BufferedImage image = code(BarcodeFormat.QR_CODE, VALUE, 300);
        Path pdf = pdfWithImageOnEveryPage(image, 3, folder.newFile("footer.pdf").toPath());
        Extractor extractor = new Extractor(Options.from(Map.of("ocr", "false", "qrCodes", "true")));

        TikaDocument root = extractor.extract(pdf);
        spew(root);

        assertThat(List.of(root.getMetadata().getValues(QR_CODES))).isEqualTo(List.of(VALUE));
    }

    @Test
    public void test_pdf_inline_image_ocr_on() throws Exception {
        Extractor extractor = new Extractor(Options.from(Map.of("qrCodes", "true")));

        assertThat(treeText(extractor, qrPdf())).contains(LINE);
    }

    @Test
    public void test_pdf_inline_image_option_off() throws Exception {
        Extractor extractor = new Extractor(Options.from(Map.of("ocr", "false")));

        assertThat(treeText(extractor, qrPdf())).doesNotContain("QR code");
    }

    @Test
    public void test_email_attachment_on_deferred_ocr_path() throws Exception {
        Path eml = emailWithQrAttachment();
        Extractor extractor = new Extractor(Options.from(Map.of("qrCodes", "true")));

        TikaDocument root = extractor.extract(eml);
        String text = spew(root);

        assertThat(extractor.ocrExecutorOrNull()).isNotNull();
        assertThat(text).contains(LINE);
        EmbeddedTikaDocument child = root.getEmbeds().get(0);
        assertThat(child.getMetadata().getValues(QR_CODES)).containsOnly(VALUE);
    }

    @Test
    public void test_deferred_ocr_values_reach_metadata_written_before_the_text() throws Exception {
        Path eml = emailWithQrAttachment();
        Extractor extractor = new Extractor(Options.from(Map.of("qrCodes", "true")));

        TikaDocument root = extractor.extract(eml);
        String output = spew(root, true);

        assertThat(output).contains("tika_metadata_qr_codes: " + VALUE);
    }

    private Path emailWithQrAttachment() throws Exception {
        String png = Base64.getMimeEncoder().encodeToString(Files.readAllBytes(qrImage("png")));
        String email = String.join("\r\n",
                "From: a@example.org",
                "To: b@example.org",
                "Subject: qr",
                "MIME-Version: 1.0",
                "Content-Type: multipart/mixed; boundary=\"b\"",
                "",
                "--b",
                "Content-Type: text/plain",
                "",
                "see attachment",
                "--b",
                "Content-Type: image/png; name=\"qr.png\"",
                "Content-Disposition: attachment; filename=\"qr.png\"",
                "Content-Transfer-Encoding: base64",
                "",
                png,
                "--b--",
                "");
        return Files.writeString(folder.newFile("qr.eml").toPath(), email);
    }

    private Path qrImage(String format) throws Exception {
        BufferedImage image = code(BarcodeFormat.QR_CODE, VALUE, 300);
        return write(image, format, folder.newFile("qr." + format).toPath());
    }

    private Path qrWbmp() throws Exception {
        BufferedImage gray = code(BarcodeFormat.QR_CODE, VALUE, 300);
        BufferedImage binary = new BufferedImage(gray.getWidth(), gray.getHeight(), BufferedImage.TYPE_BYTE_BINARY);
        for (int y = 0; y < gray.getHeight(); y++) {
            for (int x = 0; x < gray.getWidth(); x++) {
                binary.setRGB(x, y, gray.getRGB(x, y));
            }
        }
        return write(binary, "wbmp", folder.newFile("qr.wbmp").toPath());
    }

    private Path qrPdf() throws Exception {
        BufferedImage image = code(BarcodeFormat.QR_CODE, VALUE, 300);
        return pdfWithImage(image, folder.newFile("qr.pdf").toPath());
    }

    private static String read(TikaDocument document) throws Exception {
        try (Reader reader = document.getReader()) {
            return Spewer.toString(reader);
        }
    }

    private static String treeText(Extractor extractor, Path file) throws Exception {
        TikaDocument root = extractor.extract(file);
        return spew(root);
    }

    private static String spew(TikaDocument root) throws Exception {
        return spew(root, false);
    }

    private static String spew(TikaDocument root, boolean outputMetadata) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream print = new PrintStream(out, true, StandardCharsets.UTF_8);
        PrintStreamSpewer spewer = new PrintStreamSpewer(print, new FieldNames());
        spewer.outputMetadata(outputMetadata);
        spewer.write(root);
        root.getReader().close();
        return out.toString(StandardCharsets.UTF_8);
    }
}
