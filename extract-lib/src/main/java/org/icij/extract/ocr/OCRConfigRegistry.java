package org.icij.extract.ocr;

import org.apache.tika.parser.Parser;

import java.util.Locale;

public enum OCRConfigRegistry {
    // TODO: ideally we'd like to do something cleaner than this registry leveraging the SPI and other java extension
    //  features
    TESSERACT,
    TESS4J;

    public OCRConfigAdapter<? extends Parser> buildAdapter() {
        switch (this) {
            case TESSERACT -> {
                return new TesseractOCRConfigAdapter();
            }
            case TESS4J -> {
                return new Tess4JOCRConfigAdapter();
            }
            default -> throw new IllegalArgumentException();
        }
    }

    public static OCRConfigRegistry parse(final String ocrType) {
        return valueOf(ocrType.toUpperCase(Locale.ROOT));
    }
}
