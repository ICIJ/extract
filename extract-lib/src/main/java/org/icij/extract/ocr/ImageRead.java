package org.icij.extract.ocr;

import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.ocr.TesseractOCRConfig;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.nio.file.Path;

/** An image on disk that {@link PageSegModeOCRParser} reads once per page segmentation mode. */
record ImageRead(Parser delegate, Path image, Metadata metadata, ParseContext context) {
    void run(TesseractOCRConfig config, ContentHandler handler) throws IOException, SAXException, TikaException {
        context.set(TesseractOCRConfig.class, config);
        try (TikaInputStream input = TikaInputStream.get(image)) {
            delegate.parse(input, handler, metadata, context);
        }
    }
}
