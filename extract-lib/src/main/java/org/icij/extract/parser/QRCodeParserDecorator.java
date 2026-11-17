package org.icij.extract.parser;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serial;
import java.util.List;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TemporaryResources;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.Property;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.ParserDecorator;
import org.apache.tika.sax.ContentHandlerDecorator;
import org.apache.tika.sax.XHTMLContentHandler;
import org.icij.extract.qrcode.QRCodeDecoder;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

public class QRCodeParserDecorator extends ParserDecorator {
    @Serial
    private static final long serialVersionUID = 4119547702315383213L;

    public static final Property QR_CODES = Property.internalTextBag("qr_codes");

    public QRCodeParserDecorator(Parser parser) {
        super(parser);
    }

    @Override
    public void parse(InputStream stream, ContentHandler handler, Metadata metadata, ParseContext context)
            throws IOException, SAXException, TikaException {
        TemporaryResources tmp = new TemporaryResources();
        try {
            TikaInputStream tis = TikaInputStream.get(stream, tmp, metadata);
            List<String> values = QRCodeDecoder.decode(tis.getPath());
            ContentHandler target = values.isEmpty() ? handler : new QRCodeLines(handler, values, metadata);
            super.parse(tis, target, metadata, context);
        } finally {
            tmp.dispose();
        }
    }

    // The lines go just before </body> so every output format (text, HTML) keeps them inside the body.
    private static class QRCodeLines extends ContentHandlerDecorator {
        private final List<String> values;
        private final Metadata metadata;
        private boolean written = false;

        QRCodeLines(ContentHandler handler, List<String> values, Metadata metadata) {
            super(handler);
            this.values = values;
            this.metadata = metadata;
        }

        @Override
        public void endElement(String uri, String localName, String name) throws SAXException {
            boolean closesBody = XHTMLContentHandler.XHTML.equals(uri) && "body".equals(localName);
            if (closesBody && !written) {
                written = true;
                for (String value : values) {
                    writeLine(value);
                }
            }
            super.endElement(uri, localName, name);
        }

        private void writeLine(String value) throws SAXException {
            char[] line = ("QR code: " + value).toCharArray();
            super.startElement(XHTMLContentHandler.XHTML, "p", "p", new AttributesImpl());
            super.characters(line, 0, line.length);
            super.endElement(XHTMLContentHandler.XHTML, "p", "p");
            super.ignorableWhitespace(new char[] {'\n'}, 0, 1);
            metadata.add(QR_CODES, value);
        }
    }
}
