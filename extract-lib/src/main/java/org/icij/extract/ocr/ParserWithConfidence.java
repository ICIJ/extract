package org.icij.extract.ocr;

import org.apache.tika.metadata.Property;
import org.apache.tika.parser.Parser;

// TODO: do we really need an abstract class, we just need a common namespace
public abstract class ParserWithConfidence implements Parser {
    public static final Property OCR_CONFIDENCE = Property.externalReal("ocr:confidence");
}
