package org.icij.extract.ocr;

import org.apache.tika.parser.Parser;
import org.apache.tika.parser.ocr.TesseractOCRConfig;

import java.time.Duration;

public interface OCRConfigAdapter<P extends Parser> {
    void setLanguages(final String... languages);

    void setParsingTimeoutS(final int timeoutS);

    default void setOcrTimeout(final Duration duration) {
        setParsingTimeoutS((int) duration.toSeconds());
    }

    TesseractOCRConfig getConfig();

    Class<P> getParserClass();

    OCRParserAdapter<P> buildParser();
}
