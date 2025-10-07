package org.icij.extract.ocr;

import org.apache.tika.metadata.Property;
import org.apache.tika.parser.Parser;

/**
 * Empty interface for tagging implementing classes
 * It could embed some common treatments for all daughters.
 */
public interface OCRParser extends Parser {
    public static final Property OCR_USED = Property.externalBoolean("ocr:used");
    public static final Property OCR_ENGINE = Property.externalText("ocr:engine");
}
