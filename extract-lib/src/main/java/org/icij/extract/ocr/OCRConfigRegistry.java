package org.icij.extract.ocr;

import java.lang.reflect.InvocationTargetException;
import java.util.Locale;

public enum OCRConfigRegistry {
    // TODO: ideally we'd like to do something cleaner than this registry leveraging the SPI and other java extension
    //  features
    TESSERACT,
    TESS4J;

    public Class<?> getAdapterClass() {
        switch (this) {
            case TESSERACT -> {
                return TesseractOCRConfigAdapter.class;
            }
            case TESS4J -> {
                return Tess4JOCRConfigAdapter.class;
            }
            default -> throw new IllegalArgumentException();
        }
    }

    public OCRConfigAdapter<?, ?> newAdapter() throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        return (OCRConfigAdapter<?, ?>) this.getAdapterClass().getConstructor().newInstance();
    }

    public static OCRConfigRegistry parse(final String ocrType) {
        return valueOf(ocrType.toUpperCase(Locale.ROOT));
    }
}
