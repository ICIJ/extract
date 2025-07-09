package org.icij.extract.ocr;

import org.apache.tika.parser.ocr.TesseractOCRConfig;

public class Tess4JOCRConfigAdapter implements OCRConfigAdapter<Tess4JOCRParser> {
    private final TesseractOCRConfig inner;

    public Tess4JOCRConfigAdapter() {
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
    public Class<Tess4JOCRParser> getParserClass() {
        return Tess4JOCRParser.class;
    }

    @Override
    public Tess4JOCRParser buildParser() {
        return new Tess4JOCRParser();
    }
}
