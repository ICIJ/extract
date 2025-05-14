package org.icij.extract.ocr;

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

    public OCRConfigAdapter<?, ?> newAdapter() {
        try {
            return (OCRConfigAdapter<?, ?>) this.getAdapterClass().getConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new OCRConfigRegistryAdapterException(e);
        }
    }

    public static OCRConfigRegistry parse(final String ocrType) {
        return valueOf(ocrType.toUpperCase(Locale.ROOT));
    }
    public static class OCRConfigRegistryAdapterException extends RuntimeException {
        public OCRConfigRegistryAdapterException(Throwable cause) {
            super(cause);
        }
    }
}
