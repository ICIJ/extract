package org.icij.extract.ocr;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.ocr.TesseractOCRParser;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;

public class OCRAdapter extends TesseractOCRParser {
    private final OCRParser delegatedParser;

    public OCRAdapter() {this(null);}
    public OCRAdapter(OCRParser delegatedParser) {
        this.delegatedParser = delegatedParser;
    }

    @Override
    public void parse(InputStream stream, ContentHandler handler, Metadata metadata, ParseContext parseContext) throws IOException, SAXException, TikaException {
        if (delegatedParser == null) {
            super.parse(stream, handler, metadata, parseContext);
        } else {
            delegatedParser.parse(stream, handler, metadata, parseContext);
        }
    }
}
