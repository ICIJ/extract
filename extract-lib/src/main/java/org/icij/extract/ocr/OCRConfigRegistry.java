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

    public OCRConfigAdapter<?, ?> newAdapter() {
        Class<?> adapterClass = this.getAdapterClass();
        try {
            return (OCRConfigAdapter<?, ?>) adapterClass.getConstructor().newInstance();
        } catch (IllegalAccessException e) {
            throw new OCRConfigRegistryAdapterException(adapterClass + " no-arg constructor is not accessible", e);
        } catch (NoSuchMethodException e) {
            throw new OCRConfigRegistryAdapterException(adapterClass + " has no no-arg constructor", e);
        } catch (InvocationTargetException | InstantiationException e) {
            throw new OCRConfigRegistryAdapterException ("failed to instanciate " + adapterClass + " using has no no-arg constructor", e);
        }
    }

    public static OCRConfigRegistry parse(final String ocrType) {
        return valueOf(ocrType.toUpperCase(Locale.ROOT));
    }
    public static class OCRConfigRegistryAdapterException extends RuntimeException {
        public OCRConfigRegistryAdapterException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
