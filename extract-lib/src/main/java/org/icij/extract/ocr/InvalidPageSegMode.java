package org.icij.extract.ocr;

/** An ocrPageSegMode option value that is neither AUTO nor a tesseract mode that reads text. */
public class InvalidPageSegMode extends IllegalArgumentException {
    final String value;

    public InvalidPageSegMode(String value) {
        super("ocrPageSegMode \"%s\" is not AUTO or a tesseract page segmentation mode from 1 to 13 except 2"
                .formatted(value));
        this.value = value;
    }
}
