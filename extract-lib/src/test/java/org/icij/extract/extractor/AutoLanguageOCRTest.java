package org.icij.extract.extractor;

import org.apache.tika.metadata.Metadata;
import org.icij.extract.document.TikaDocument;
import org.icij.spewer.Spewer;
import org.icij.task.Options;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Reader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.fest.assertions.Assertions.assertThat;
import static org.icij.extract.ocr.AutoLanguageOCRParser.OCR_MODEL;
import static org.icij.extract.ocr.AutoLanguageOCRParser.OCR_SCRIPT;
import static org.icij.extract.ocr.ParserWithConfidence.OCR_CONFIDENCE;

@RunWith(Parameterized.class)
public class AutoLanguageOCRTest {
    @Parameterized.Parameters(name = "{0} {1}")
    public static List<Object[]> fixtures() {
        List<Object[]> rows = new ArrayList<>();
        for (String backend : List.of("TESSERACT", "TESS4J")) {
            rows.add(new Object[] {backend, "latin", "Latin", "script/Latin", List.of("city council published", "public library")});
            rows.add(new Object[] {backend, "cyrillic", "Cyrillic", "script/Cyrillic+script/Latin", List.of("Городской совет", "публичной библиотеке")});
            rows.add(new Object[] {backend, "arabic", "Arabic", "script/Arabic+script/Latin", List.of("مجلس المدينة", "المكتبة العامة")});
            rows.add(new Object[] {backend, "hans", "Han", "script/HanS+script/Latin", List.of("预算报告", "公共图书馆")});
            rows.add(new Object[] {backend, "japanese", "Japanese", "script/Japanese+script/Latin", List.of("予算", "公共図書館")});
            rows.add(new Object[] {backend, "hangul", "Korean", "script/Hangul+script/Latin", List.of("예산", "월요일")});
        }
        return rows;
    }

    @Parameterized.Parameter(0) public String backend;
    @Parameterized.Parameter(1) public String fixture;
    @Parameterized.Parameter(2) public String script;
    @Parameterized.Parameter(3) public String model;
    @Parameterized.Parameter(4) public List<String> words;

    @Test
    public void test_reads_the_image_in_its_script() throws Exception {
        // Given
        Extractor extractor = new Extractor(Options.from(Map.of("ocrType", backend)));
        // When
        TikaDocument document = extractor.extract(Paths.get(getClass().getResource("/documents/ocr/auto/" + fixture + ".png").getPath()));
        String text;
        try (Reader reader = document.getReader()) {
            text = Spewer.toString(reader);
        }
        // Then
        Metadata metadata = document.getMetadata();
        assertThat(metadata.get(OCR_SCRIPT)).isEqualTo(script);
        assertThat(metadata.get(OCR_MODEL)).isEqualTo(model);
        assertThat(Double.parseDouble(metadata.get(OCR_CONFIDENCE))).isGreaterThanOrEqualTo(0.6);
        for (String word : words) {
            assertThat(squash(text)).contains(squash(word));
        }
    }

    static String squash(String text) {
        return text.replaceAll("\\s+", "");
    }
}
