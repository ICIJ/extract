package org.icij.extract.ocr;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

public class OCRParserAdapter<P extends Parser> implements Parser {
    private final P delegatedParser;

    public OCRParserAdapter(P delegatedParser) {
        this.delegatedParser = delegatedParser;
    }

    @Override
    public Set<MediaType> getSupportedTypes(ParseContext parseContext) {
        return this.delegatedParser.getSupportedTypes(parseContext);
    }

    @Override
    public void parse(InputStream stream, ContentHandler handler, Metadata metadata, ParseContext parseContext) throws IOException, SAXException, TikaException {
        assert delegatedParser != null;
        metadata.set(OCRParser.OCR_USED, true);
        delegatedParser.parse(stream, handler, metadata, parseContext);
    }
}
