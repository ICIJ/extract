package org.icij.extract.parser;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serial;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TemporaryResources;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.Property;
import org.apache.tika.metadata.TikaCoreProperties;
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
        if (context.get(QRCodeParserDecorator.class) != null) {
            super.parse(stream, handler, metadata, context);
            return;
        }
        context.set(QRCodeParserDecorator.class, this);
        TemporaryResources tmp = new TemporaryResources();
        try {
            TikaInputStream tis = TikaInputStream.get(stream, tmp, metadata);
            List<String> values = QRCodeDecoder.decode(tis.getPath());
            if (values.isEmpty()) {
                super.parse(tis, handler, metadata, context);
                return;
            }
            addToMetadata(values, metadata);
            addToRenderedPagesTarget(metadata, context);
            parseWithLines(tis, new QRCodeLines(handler, values), metadata, context);
        } finally {
            tmp.dispose();
            context.set(QRCodeParserDecorator.class, null);
        }
    }

    private static void addToMetadata(List<String> values, Metadata metadata) {
        for (String value : values) {
            metadata.add(QR_CODES, value);
        }
    }

    private static void addToRenderedPagesTarget(Metadata metadata, ParseContext context) {
        String resourceType = metadata.get(TikaCoreProperties.EMBEDDED_RESOURCE_TYPE);
        boolean isRenderedPage = TikaCoreProperties.EmbeddedResourceType.RENDERING.name().equals(resourceType);
        RenderedPagesQRCodeDecorator.Target target = context.get(RenderedPagesQRCodeDecorator.Target.class);
        if (isRenderedPage && target != null) {
            copyQrCodes(metadata, target.metadata());
        }
    }

    // A concatenated or inline embed has no document of its own, so its QR code values belong to the
    // document holding its text. A code repeated on every page (a footer) is kept once.
    public static void copyQrCodes(final Metadata embed, final Metadata holder) {
        final Set<String> known = new HashSet<>(Arrays.asList(holder.getValues(QR_CODES)));
        for (final String value : embed.getValues(QR_CODES)) {
            if (known.add(value)) {
                holder.add(QR_CODES, value);
            }
        }
    }

    // The pixels decoded fine even when the wrapped parser fails (corrupt EXIF, OCR crash), so the lines
    // are still written on a best-effort basis before the failure goes up.
    private void parseWithLines(TikaInputStream tis, QRCodeLines lines, Metadata metadata, ParseContext context)
            throws IOException, SAXException, TikaException {
        try {
            super.parse(tis, lines, metadata, context);
        } catch (IOException | SAXException | TikaException | RuntimeException | Error parseFailure) {
            writeLinesBestEffort(lines, parseFailure);
            throw parseFailure;
        }
        // An OCR cache hit replays the text without a </body>.
        lines.writeOnce();
    }

    private static void writeLinesBestEffort(QRCodeLines lines, Throwable parseFailure) {
        try {
            lines.writeOnce();
        } catch (SAXException | RuntimeException writeFailure) {
            parseFailure.addSuppressed(writeFailure);
        }
    }

    // The lines go just before </body> so every output format (text, HTML) keeps them inside the body.
    private static class QRCodeLines extends ContentHandlerDecorator {
        private final List<String> values;
        private boolean written = false;

        QRCodeLines(ContentHandler handler, List<String> values) {
            super(handler);
            this.values = values;
        }

        @Override
        public void endElement(String uri, String localName, String name) throws SAXException {
            boolean closesBody = XHTMLContentHandler.XHTML.equals(uri) && "body".equals(localName);
            if (closesBody) {
                writeOnce();
            }
            super.endElement(uri, localName, name);
        }

        void writeOnce() throws SAXException {
            if (written) {
                return;
            }
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
