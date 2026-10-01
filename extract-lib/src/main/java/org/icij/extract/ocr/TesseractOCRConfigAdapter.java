package org.icij.extract.ocr;

import org.apache.tika.exception.TikaConfigException;
import org.apache.tika.parser.ocr.TesseractOCRConfig;
import org.apache.tika.parser.ocr.TesseractOCRParser;

import java.util.HashMap;
import java.util.Set;

public class TesseractOCRConfigAdapter implements OCRConfigAdapter<TesseractOCRParser> {
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

    @Override
    public OCRParserAdapter<TesseractOCRParser> buildParser() {
        try {
            TesseractOCRParser tesseractOCRParser = new TesseractOCRParser();
            tesseractOCRParser.initialize(new HashMap<>());
            return new OCRParserAdapter<>(tesseractOCRParser);
        } catch (TikaConfigException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Set<String> installedModels() {
        try {
            TesseractOCRParser probe = new TesseractOCRParser();
            probe.setPreloadLangs(true);
            probe.initialize(new HashMap<>());
            return probe.getLangs();
        } catch (TikaConfigException e) {
            return Set.of();
        }
    }
}
