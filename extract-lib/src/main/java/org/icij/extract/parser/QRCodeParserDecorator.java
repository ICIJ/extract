package org.icij.extract.parser;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serial;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.imageio.ImageIO;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TemporaryResources;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.Property;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.ParserDecorator;
import org.apache.tika.sax.ContentHandlerDecorator;
import org.apache.tika.sax.XHTMLContentHandler;
import org.icij.extract.qrcode.QRCodeDecoder;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

public class QRCodeParserDecorator extends ParserDecorator {
    @Serial
    private static final long serialVersionUID = 4119547702315383213L;

    public static final Property QR_CODES = Property.internalTextBag("qr_codes");

    // Every type an ImageIO reader decodes, so an image no parser claims is still decoded.
    private static final Set<MediaType> IMAGEIO_TYPES = imageIOTypes();

    /**
     * Wraps the parser that picks a parser per detected type: every image, embedded or not, passes through
     * it, as do the pages a rendering PDF OCR strategy hands to OCR.
     */
    public QRCodeParserDecorator(Parser parser) {
        super(parser);
    }

    /** Adds the values {@code to} does not hold yet, so an image repeated in a document counts once. */
    public static void addQrCodes(Metadata from, Metadata to) {
        String[] current = to.getValues(QR_CODES);
        List<String> currentList = Arrays.asList(current);
        Set<String> existing = new HashSet<>(currentList);
        for (String value : from.getValues(QR_CODES)) {
            if (existing.add(value)) {
                to.add(QR_CODES, value);
            }
        }
    }

    @Override
    public Set<MediaType> getSupportedTypes(ParseContext context) {
        Set<MediaType> wrapped = super.getSupportedTypes(context);
        Set<MediaType> types = new HashSet<>(wrapped);
        types.addAll(IMAGEIO_TYPES);
        return types;
    }

    @Override
    public void parse(InputStream stream, ContentHandler handler, Metadata metadata, ParseContext context)
            throws IOException, SAXException, TikaException {
        // The image parser hands the image it parses back to the context parser for OCR: decode it once.
        // Other nested images (embedded in an EMF) are decoded.
        boolean ocrHandoff = context.get(Decoding.class) != null && isOcrType(metadata);
        if (ocrHandoff || !isImage(metadata)) {
            super.parse(stream, handler, metadata, context);
            return;
        }
        Decoding outer = context.get(Decoding.class);
        context.set(Decoding.class, Decoding.INSTANCE);
        TemporaryResources tmp = new TemporaryResources();
        try {
            TikaInputStream tis = TikaInputStream.get(stream, tmp, metadata);
            Path image = tis.getPath();
            String contentType = metadata.get(Metadata.CONTENT_TYPE);
            List<String> values = QRCodeDecoder.decode(image, contentType);
            for (String value : values) {
                metadata.add(QR_CODES, value);
            }
            if (values.isEmpty()) {
                super.parse(tis, handler, metadata, context);
                return;
            }
            QRCodeLines lines = new QRCodeLines(handler, values);
            super.parse(tis, lines, metadata, context);
            lines.writeIfMissing(metadata);
        } finally {
            context.set(Decoding.class, outer);
            tmp.dispose();
        }
    }

    private static boolean isImage(Metadata metadata) {
        String override = metadata.get(TikaCoreProperties.CONTENT_TYPE_PARSER_OVERRIDE);
        String detected = metadata.get(Metadata.CONTENT_TYPE);
        String contentType = override != null ? override : detected;
        MediaType type = MediaType.parse(contentType);
        return type != null && "image".equals(type.getType());
    }

    private static boolean isOcrType(Metadata metadata) {
        String override = metadata.get(TikaCoreProperties.CONTENT_TYPE_PARSER_OVERRIDE);
        return override != null && override.startsWith("image/ocr-");
    }

    private static Set<MediaType> imageIOTypes() {
        Set<MediaType> types = new HashSet<>();
        for (String mime : ImageIO.getReaderMIMETypes()) {
            MediaType type = MediaType.parse(mime);
            if (type != null) {
                types.add(type);
            }
        }
        return Set.copyOf(types);
    }

    private enum Decoding { INSTANCE }

    // The lines go just before </body> so every output format (text, HTML) keeps them inside the body.
    private static class QRCodeLines extends ContentHandlerDecorator {
        private final List<String> values;
        private boolean bodyOpen = false;
        private boolean written = false;

        QRCodeLines(ContentHandler handler, List<String> values) {
            super(handler);
            this.values = values;
        }

        @Override
        public void startElement(String uri, String localName, String name, Attributes atts) throws SAXException {
            if (isBody(uri, localName)) {
                bodyOpen = true;
            }
            super.startElement(uri, localName, name, atts);
        }

        @Override
        public void endElement(String uri, String localName, String name) throws SAXException {
            if (isBody(uri, localName) && !written) {
                writeLines();
            }
            super.endElement(uri, localName, name);
        }

        // Parsers that answer from the OCR cache or give up early never close the body, or never open it.
        void writeIfMissing(Metadata metadata) throws SAXException {
            if (written) {
                return;
            }
            if (bodyOpen) {
                writeLines();
                return;
            }
            XHTMLContentHandler xhtml = new XHTMLContentHandler(this, metadata);
            xhtml.startDocument();
            xhtml.endDocument();
        }

        private static boolean isBody(String uri, String localName) {
            return XHTMLContentHandler.XHTML.equals(uri) && "body".equals(localName);
        }

        private void writeLines() throws SAXException {
            written = true;
            for (String value : values) {
                writeLine(value);
            }
        }

        private void writeLine(String value) throws SAXException {
            char[] line = ("QR code: " + value).toCharArray();
            super.startElement(XHTMLContentHandler.XHTML, "p", "p", new AttributesImpl());
            super.characters(line, 0, line.length);
            super.endElement(XHTMLContentHandler.XHTML, "p", "p");
            super.ignorableWhitespace(new char[] {'\n'}, 0, 1);
        }
    }
}
