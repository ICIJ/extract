package org.icij.extract.extractor;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.tika.parser.EmptyParser;
import org.apache.tika.parser.Parser;
import org.icij.extract.document.TikaDocument;
import org.icij.extract.ocr.TesseractOCRConfigAdapter;
import org.icij.spewer.Spewer;
import org.icij.task.Options;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.fest.assertions.Assertions.assertThat;
import static org.icij.extract.extractor.AutoLanguageOCRTest.squash;
import static org.icij.extract.ocr.AutoLanguageOCRParser.OCR_MODEL;
import static org.icij.extract.ocr.AutoLanguageOCRParser.OCR_SCRIPT;

public class ExtractorAutoLanguageTest {
    private static final String ALL = "script/Latin+script/HanS+script/Cyrillic+script/Arabic+script/Japanese+script/Hangul";

    @Test
    public void test_mixed_image_keeps_every_script() throws Exception {
        for (String backend : List.of("TESSERACT", "TESS4J")) {
            // Given
            Extractor extractor = new Extractor(Options.from(Map.of("ocrType", backend)));
            // When
            String text = textOf(extractor.extract(path("/documents/ocr/auto/mixed.png")));
            // Then
            assertThat(squash(text)).as(backend).contains("预算报告");
            assertThat(squash(text)).as(backend).contains("Городскойсовет");
            assertThat(squash(text)).as(backend).contains("citycouncil");
        }
    }

    @Test
    public void test_retry_confidence_zero_keeps_the_first_pass() throws Exception {
        // Given
        Extractor extractor = new Extractor(Options.from(Map.of("ocrRetryConfidence", "0")));
        // When
        TikaDocument document = extractor.extract(path("/documents/ocr/auto/mixed.png"));
        textOf(document);
        // Then
        assertThat(document.getMetadata().get(OCR_MODEL)).isNotNull();
        assertThat(document.getMetadata().get(OCR_MODEL)).isNotEqualTo(ALL);
    }

    @Test
    public void test_too_little_text_for_detection_reads_as_latin() throws Exception {
        // Given
        Extractor extractor = new Extractor();
        // When
        TikaDocument document = extractor.extract(path("/documents/ocr/simple.tiff"));
        String text = textOf(document);
        // Then
        assertThat(text).contains("HEAVY");
        assertThat(text).contains("METAL");
        assertThat(document.getMetadata().get(OCR_SCRIPT)).isNull();
        assertThat(document.getMetadata().get(OCR_MODEL)).isEqualTo("script/Latin");
    }

    @Test
    public void test_failed_detection_leaves_no_osd_file_behind() throws Exception {
        // Given
        Extractor extractor = new Extractor();
        long before = osdFiles();
        // When
        textOf(extractor.extract(path("/documents/ocr/simple.tiff")));
        // Then
        assertThat(osdFiles()).isEqualTo(before);
    }

    @Test
    public void test_every_page_of_a_multipage_tiff_is_read() throws Exception {
        // Given
        Extractor extractor = new Extractor();
        // When
        TikaDocument document = extractor.extract(path("/documents/ocr/test_tiff_multipage.tif"));
        String text = textOf(document);
        // Then
        assertThat(document.getMetadata().get(OCR_MODEL)).isNotNull();
        assertThat(text.replaceAll("[^A-Za-z0-9]", " ")).contains("Page 2");
    }

    @Test
    public void test_explicit_language_skips_routing() throws Exception {
        // Given
        Extractor extractor = new Extractor(Options.from(Map.of("ocrLanguage", "eng")));
        // When
        TikaDocument document = extractor.extract(path("/documents/ocr/simple.tiff"));
        String text = textOf(document);
        // Then
        assertThat(text.trim()).isEqualTo("HEAVY\nMETAL");
        assertThat(document.getMetadata().get(OCR_MODEL)).isNull();
    }

    @Test
    public void test_setting_a_language_after_construction_skips_routing() throws Exception {
        // Given
        Extractor extractor = new Extractor();
        extractor.setOcrLanguage("eng");
        // When
        TikaDocument document = extractor.extract(path("/documents/ocr/simple.tiff"));
        String text = textOf(document);
        // Then
        assertThat(text.trim()).isEqualTo("HEAVY\nMETAL");
        assertThat(document.getMetadata().get(OCR_MODEL)).isNull();
    }

    @Test
    public void test_retry_confidence_is_clamped_to_0_100() {
        assertThat(new Extractor(Options.from(Map.of("ocrRetryConfidence", "150"))).getOcrRetryConfidence()).isEqualTo(100);
        assertThat(new Extractor(Options.from(Map.of("ocrRetryConfidence", "-5"))).getOcrRetryConfidence()).isEqualTo(0);
        assertThat(new Extractor().getOcrRetryConfidence()).isEqualTo(60);
    }

    @Test
    public void test_missing_models_keep_plain_ocr_and_warn_once() {
        // Given
        TesseractOCRConfigAdapter partial = new TesseractOCRConfigAdapter() {
            @Override
            public Set<String> installedModels() {
                return Set.of("osd", "script/Latin");
            }
        };
        Extractor extractor = new Extractor();
        Logger log = (Logger) LoggerFactory.getLogger(Extractor.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        log.addAppender(appender);
        // When
        Parser installed;
        try {
            installed = extractor.withAutoLanguage(partial, EmptyParser.INSTANCE);
        } finally {
            log.detachAppender(appender);
        }
        // Then
        assertThat(installed).isSameAs(EmptyParser.INSTANCE);
        List<ILoggingEvent> warnings = appender.list.stream().filter(e -> e.getLevel() == Level.WARN).toList();
        assertThat(warnings).hasSize(1);
        assertThat(warnings.get(0).getFormattedMessage()).contains("script/HanS");
    }

    @Test
    public void test_missing_models_are_probed_and_warned_once_per_adapter_class() {
        // Given
        AtomicInteger probes = new AtomicInteger();
        TesseractOCRConfigAdapter partial = new TesseractOCRConfigAdapter() {
            @Override
            public Set<String> installedModels() {
                probes.incrementAndGet();
                return Set.of("osd");
            }
        };
        Logger log = (Logger) LoggerFactory.getLogger(Extractor.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        log.addAppender(appender);
        // When
        try {
            new Extractor().withAutoLanguage(partial, EmptyParser.INSTANCE);
            new Extractor().withAutoLanguage(partial, EmptyParser.INSTANCE);
        } finally {
            log.detachAppender(appender);
        }
        // Then
        assertThat(probes.get()).isEqualTo(1);
        assertThat(appender.list.stream().filter(e -> e.getLevel() == Level.WARN).toList()).hasSize(1);
    }

    private static long osdFiles() throws IOException {
        Path tmp = Path.of(System.getProperty("java.io.tmpdir"));
        try (DirectoryStream<Path> files = Files.newDirectoryStream(tmp, "apache-tika-*.osd")) {
            long count = 0;
            for (Path ignored : files) {
                count++;
            }
            return count;
        }
    }

    private static Path path(String resource) {
        return Paths.get(ExtractorAutoLanguageTest.class.getResource(resource).getPath());
    }

    private static String textOf(TikaDocument document) throws IOException {
        try (Reader reader = document.getReader()) {
            return Spewer.toString(reader);
        }
    }
}
