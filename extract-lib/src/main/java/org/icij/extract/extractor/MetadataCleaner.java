package org.icij.extract.extractor;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDDocumentInformation;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.icij.extract.document.TikaDocument;
import org.icij.extract.document.TikaDocumentSource;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class MetadataCleaner {
    private final ContentExtractor parser;

    public MetadataCleaner() {
        this.parser = new ContentExtractor();
    }

    public TikaDocumentSource extract(TikaDocument rootDocument) throws SAXException, TikaException, IOException {
        ParseContext context = new ParseContext();
        ContentHandler handler = new BodyContentHandler(-1);
        context.set(Parser.class, parser);

        parser.parse(new FileInputStream(rootDocument.getPath().toFile()), handler, rootDocument.getMetadata(), context);

        return parser.document;
    }

    static class ContentExtractor extends AutoDetectParser {
        TikaDocumentSource document = null;
        @Override
        public void parse(InputStream stream, ContentHandler handler, Metadata metadata, ParseContext context) throws IOException, SAXException, TikaException {
            PDDocument document = PDDocument.load(stream);
            PDDocumentInformation information = document.getDocumentInformation();
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            document.getDocumentCatalog().setMetadata(null);
            if(information != null) {
                document.setDocumentInformation(new PDDocumentInformation());
                document.save(outputStream);
            }
            document.close();
            this.document = new TikaDocumentSource(metadata, outputStream.toByteArray());
        }
    }
}
