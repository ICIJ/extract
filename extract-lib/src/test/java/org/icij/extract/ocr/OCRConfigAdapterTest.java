package org.icij.extract.ocr;

import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

public class OCRConfigAdapterTest {
    @Test
    public void test_tesseract_lists_osd_and_script_models() {
        assertThat(new TesseractOCRConfigAdapter().installedModels()).contains("osd", "script/Latin", "script/HanS");
    }

    @Test
    public void test_tess4j_lists_osd_and_script_models() {
        assertThat(new Tess4JOCRConfigAdapter().installedModels()).contains("osd", "script/Latin", "script/HanS");
    }
}
