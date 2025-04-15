package org.icij.extract.parser;

import java.lang.reflect.InvocationTargetException;
import java.util.Locale;

public enum OCRConfig {
    // TODO: ideally we'd like to do something cleaner than this registry leveraging the SPI and other java extension
    //  features
    TESSERACT;

    public Class<?> getAdapterClass() {
        switch (this) {
            case TESSERACT -> {
                return TesseractOCRConfigAdapter.class;
            }
            default -> throw new IllegalArgumentException();
        }
    }

    public OCRConfigAdapter<?, ?> newAdapter() throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        return (OCRConfigAdapter<?, ?>) this.getAdapterClass().getConstructor().newInstance();
    }

    public static OCRConfig parse(final String ocrType) {
        return valueOf(ocrType.toUpperCase(Locale.ROOT));
    }
}
