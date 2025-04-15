package org.icij.extract.parser;

import org.apache.tika.parser.ocr.TesseractOCRConfig;
import org.apache.tika.parser.ocr.TesseractOCRParser;

public class TesseractOCRConfigAdapter implements OCRConfigAdapter<TesseractOCRConfig, TesseractOCRParser> {
    private final TesseractOCRConfig inner;

    public TesseractOCRConfigAdapter() {
        this.inner = new TesseractOCRConfig();
    }

    @Override
    public void setLanguages(final String... languages) {
        inner.setLanguage(String.join("+", languages));
    }
    
    @Override
    public void setParsingTimeoutS(final int timeoutS) {
        inner.setTimeoutSeconds(timeoutS);
    }

    @Override
    public TesseractOCRConfig getConfig() {
        return inner;
    }

    @Override
    public Class<TesseractOCRParser> getParserClass() {
        return TesseractOCRParser.class;
    }
}
