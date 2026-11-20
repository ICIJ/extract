package org.icij.extract.extractor;

import static org.fest.assertions.Assertions.assertThat;
import static org.icij.extract.parser.QRCodeParserDecorator.QR_CODES;
import static org.icij.extract.qrcode.QRCodeImages.canvas;
import static org.icij.extract.qrcode.QRCodeImages.code;
import static org.icij.extract.qrcode.QRCodeImages.pdfWithImage;
import static org.icij.extract.qrcode.QRCodeImages.pdfWithImageAndAttachment;
import static org.icij.extract.qrcode.QRCodeImages.pdfWithImageOnEveryPage;
import static org.icij.extract.ocr.OCRParser.OCR_PARSER;
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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.icij.extract.document.EmbeddedTikaDocument;
import org.icij.extract.document.TikaDocument;
import org.icij.extract.ocr.OCRConfigRegistry;
import org.icij.extract.ocr.Tess4JOCRParser;
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
    public void test_auto_strategy_warns_that_only_rendered_pages_are_read() {
        List<String> warnings = qrCodesWarnings(Options.from(Map.of("qrCodes", "true", "ocrStrategy", "AUTO")));

        assertThat(warnings).isNotEmpty();
    }

    @Test
    public void test_ocr_only_strategy_does_not_warn() {
        List<String> warnings = qrCodesWarnings(Options.from(Map.of("qrCodes", "true", "ocrStrategy", "OCR_ONLY")));

        assertThat(warnings).isEmpty();
    }

    @Test
    public void test_jp2_ocr_on() throws Exception {
        Extractor extractor = new Extractor(Options.from(Map.of("qrCodes", "true")));

        TikaDocument document = extractor.extract(qrJp2());
        String text = read(document);

        assertThat(count(text, LINE)).isEqualTo(1);
        assertThat(List.of(document.getMetadata().getValues(QR_CODES))).isEqualTo(List.of(VALUE));
    }

    @Test
    public void test_jp2_ocr_off() throws Exception {
        Extractor extractor = new Extractor(Options.from(Map.of("ocr", "false", "qrCodes", "true")));

        TikaDocument document = extractor.extract(qrJp2());
        String text = read(document);

        assertThat(count(text, LINE)).isEqualTo(1);
        assertThat(List.of(document.getMetadata().getValues(QR_CODES))).isEqualTo(List.of(VALUE));
    }

    @Test
    public void test_jp2_ocr_disabled_after_qr_codes_enabled() throws Exception {
        Extractor extractor = new Extractor(Options.from(Map.of("qrCodes", "true")));
        extractor.disableOcr();

        TikaDocument document = extractor.extract(qrJp2());
        String text = read(document);

        assertThat(count(text, LINE)).isEqualTo(1);
        assertThat(List.of(document.getMetadata().getValues(QR_CODES))).isEqualTo(List.of(VALUE));
    }

    @Test
    public void test_rendered_page_line_ocr_only() throws Exception {
        Extractor extractor = new Extractor(Options.from(Map.of("qrCodes", "true", "ocrStrategy", "OCR_ONLY")));

        assertThat(count(treeText(extractor, qrPdf()), LINE)).isEqualTo(1);
    }

    @Test
    public void test_ocr_reconfig_uses_the_new_engine_once() throws Exception {
        Extractor extractor = new Extractor(Options.from(Map.of("qrCodes", "true")));
        extractor.setOcrConfig(OCRConfigRegistry.TESS4J.buildAdapter());

        TikaDocument document = extractor.extract(qrJp2());
        String text = read(document);

        assertThat(count(text, LINE)).isEqualTo(1);
        assertThat(document.getMetadata().get(OCR_PARSER)).isEqualTo(Tess4JOCRParser.class.getName());
    }

    @Test
    public void test_ocr_disabled_after_qr_codes_enabled_runs_no_ocr() throws Exception {
        Extractor extractor = new Extractor(Options.from(Map.of("qrCodes", "true")));
        extractor.disableOcr();

        TikaDocument document = extractor.extract(qrImage("png"));
        read(document);

        assertThat(document.getMetadata().get(OCR_PARSER)).isNull();
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

        assertThat(count(text, LINE)).isEqualTo(1);
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
        Path file = qrWbmp();
        read(extractor.extract(file));

        TikaDocument warm = extractor.extract(file);
        String text = read(warm);

        assertThat(text).contains(LINE);
        assertThat(warm.getMetadata().getValues(QR_CODES)).containsOnly(VALUE);
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
    public void test_transcoded_image_ocr_off() throws Exception {
        Extractor extractor = new Extractor(Options.from(Map.of("ocr", "false", "qrCodes", "true")));

        TikaDocument document = extractor.extract(qrPcx());
        String text = read(document);

        assertThat(text).contains(LINE);
        assertThat(document.getMetadata().getValues(QR_CODES)).containsOnly(VALUE);
    }

    @Test
    public void test_transcoded_image_ocr_disabled_after_qr_codes_enabled() throws Exception {
        Extractor extractor = new Extractor(Options.from(Map.of("qrCodes", "true")));
        extractor.disableOcr();

        TikaDocument document = extractor.extract(qrPcx());
        String text = read(document);

        assertThat(text).contains(LINE);
        assertThat(document.getMetadata().getValues(QR_CODES)).containsOnly(VALUE);
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

    private Path qrJp2() throws Exception {
        BufferedImage image = code(BarcodeFormat.QR_CODE, VALUE, 300);
        Path file = write(image, "jpeg2000", folder.newFile("qr.jp2").toPath());
        assertThat(Files.size(file)).isGreaterThan(0L);
        return file;
    }

    private static int count(String text, String line) {
        return text.split(Pattern.quote(line), -1).length - 1;
    }

    private static List<String> qrCodesWarnings(Options<String> options) {
        Logger extractorLogger = (Logger) LoggerFactory.getLogger(Extractor.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        extractorLogger.addAppender(appender);
        try {
            new Extractor(options);
        } finally {
            extractorLogger.detachAppender(appender);
        }
        return appender.list.stream()
                .filter(event -> event.getLevel() == Level.WARN)
                .map(ILoggingEvent::getFormattedMessage)
                .filter(message -> message.contains("qrCodes"))
                .toList();
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

    // Tika's ImageParser takes WBMP when OCR is off, so a 1-bit PCX stands in for JBIG2 scans there.
    private Path qrPcx() throws Exception {
        BufferedImage image = code(BarcodeFormat.QR_CODE, VALUE, 300);
        int width = image.getWidth();
        int height = image.getHeight();
        int bytesPerLine = ((width + 15) / 16) * 2;
        ByteBuffer header = ByteBuffer.allocate(128).order(ByteOrder.LITTLE_ENDIAN);
        header.put((byte) 10).put((byte) 5).put((byte) 1).put((byte) 1);
        header.putShort((short) 0).putShort((short) 0).putShort((short) (width - 1)).putShort((short) (height - 1));
        header.putShort((short) 72).putShort((short) 72);
        header.put(new byte[] {0, 0, 0, (byte) 255, (byte) 255, (byte) 255});
        header.position(65);
        header.put((byte) 1).putShort((short) bytesPerLine).putShort((short) 1);
        ByteArrayOutputStream pcx = new ByteArrayOutputStream();
        pcx.write(header.array());
        for (int y = 0; y < height; y++) {
            byte[] line = new byte[bytesPerLine];
            for (int x = 0; x < width; x++) {
                boolean white = (image.getRGB(x, y) & 0xff) > 127;
                if (white) {
                    line[x / 8] |= (byte) (0x80 >> (x % 8));
                }
            }
            for (byte packed : line) {
                pcx.write(0xC1);
                pcx.write(packed);
            }
        }
        return Files.write(folder.newFile("qr.pcx").toPath(), pcx.toByteArray());
    }

    @Test
    public void test_rendered_page_values_reach_the_pdf_metadata() throws Exception {
        Extractor extractor = new Extractor(Options.from(Map.of("qrCodes", "true", "ocrStrategy", "OCR_ONLY")));

        TikaDocument root = extractor.extract(qrPdf());
        spew(root);

        assertThat(List.of(root.getMetadata().getValues(QR_CODES))).isEqualTo(List.of(VALUE));
    }

    @Test
    public void test_code_repeated_on_every_rendered_page_is_kept_once() throws Exception {
        BufferedImage image = code(BarcodeFormat.QR_CODE, VALUE, 300);
        Path pdf = pdfWithImageOnEveryPage(image, 3, folder.newFile("footer.pdf").toPath());
        Extractor extractor = new Extractor(Options.from(Map.of("qrCodes", "true", "ocrStrategy", "OCR_ONLY")));

        TikaDocument root = extractor.extract(pdf);
        spew(root);

        assertThat(List.of(root.getMetadata().getValues(QR_CODES))).isEqualTo(List.of(VALUE));
    }

    @Test
    public void test_rendered_page_values_skip_pdf_attachments() throws Exception {
        String other = "https://example.org/qr/attachment";
        Path attachment = write(code(BarcodeFormat.QR_CODE, other, 300), "png", folder.newFile("attached.png").toPath());
        BufferedImage page = code(BarcodeFormat.QR_CODE, VALUE, 300);
        Path pdf = pdfWithImageAndAttachment(page, attachment, folder.newFile("attached.pdf").toPath());
        Extractor extractor = new Extractor(Options.from(Map.of("qrCodes", "true", "ocrStrategy", "OCR_ONLY")));

        TikaDocument root = extractor.extract(pdf);
        spew(root);

        assertThat(List.of(root.getMetadata().getValues(QR_CODES))).isEqualTo(List.of(VALUE));
        assertThat(root.getEmbeds().get(0).getMetadata().getValues(QR_CODES)).containsOnly(other);
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
