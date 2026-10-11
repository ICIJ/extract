package org.icij.extract.ocr;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TemporaryResources;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.Property;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.ocr.TesseractOCRConfig;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;
import org.xml.sax.helpers.DefaultHandler;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Reads an image with each tesseract page segmentation mode in turn and keeps the first read that finds
 * text, so an image without a page layout is not left blank by mode 1.
 */
public class PageSegModeOCRParser implements Parser {
    public static final Property OCR_PAGE_SEG_MODE = Property.externalInteger("ocr:page_seg_mode");
    private static final List<String> AUTO = List.of("1", "6", "11");

    private final Parser delegate;
    private final List<String> modes;

    public PageSegModeOCRParser(Parser delegate, List<String> modes) {
        this.delegate = delegate;
        this.modes = modes;
    }

    public static List<String> parseModes(String value) {
        String mode = value.strip();
        if (mode.equalsIgnoreCase("AUTO")) {
            return AUTO;
        }
        // 0 only detects orientation and script, 2 is not implemented in tesseract.
        if (mode.matches("1|[3-9]|1[0-3]")) {
            return List.of(mode);
        }
        throw new IllegalArgumentException("ocrPageSegMode \"" + value
                + "\" is not AUTO or a tesseract page segmentation mode from 1 to 13 except 2");
    }

    @Override
    public Set<MediaType> getSupportedTypes(ParseContext context) {
        return delegate.getSupportedTypes(context);
    }

    @Override
    public void parse(InputStream stream, ContentHandler handler, Metadata metadata, ParseContext context)
            throws IOException, SAXException, TikaException {
        TesseractOCRConfig callerConfig = context.get(TesseractOCRConfig.class);
        TesseractOCRConfig base = callerConfig == null ? new TesseractOCRConfig() : callerConfig;
        try (TemporaryResources tmp = new TemporaryResources()) {
            Path image = TikaInputStream.get(stream, tmp, metadata).getPath();
            if (AutoLanguageOCRParser.readsAsIs(base, Files.size(image))) {
                run(image, base, handler, metadata, context);
                return;
            }
            for (String mode : modes) {
                TesseractOCRConfig config = SerializationUtils.clone(base);
                config.setPageSegMode(mode);
                SaxRecorder read = new SaxRecorder();
                run(image, config, read, metadata, context);
                if (read.hasText || mode.equals(modes.get(modes.size() - 1))) {
                    read.replayTo(handler);
                    metadata.set(OCR_PAGE_SEG_MODE, Integer.parseInt(mode));
                    return;
                }
            }
        } finally {
            context.set(TesseractOCRConfig.class, callerConfig);
        }
    }

    private void run(Path image, TesseractOCRConfig config, ContentHandler handler, Metadata metadata,
                     ParseContext context) throws IOException, SAXException, TikaException {
        context.set(TesseractOCRConfig.class, config);
        try (TikaInputStream input = TikaInputStream.get(image)) {
            delegate.parse(input, handler, metadata, context);
        }
    }

    // Only body text counts: the head holds the document title, which is not what tesseract read.
    private static class SaxRecorder extends DefaultHandler {
        private final List<SaxEvent> events = new ArrayList<>();
        private boolean inBody;
        private boolean hasText;

        @Override
        public void startDocument() {
            events.add(ContentHandler::startDocument);
        }

        @Override
        public void endDocument() {
            events.add(ContentHandler::endDocument);
        }

        @Override
        public void startPrefixMapping(String prefix, String uri) {
            events.add(handler -> handler.startPrefixMapping(prefix, uri));
        }

        @Override
        public void endPrefixMapping(String prefix) {
            events.add(handler -> handler.endPrefixMapping(prefix));
        }

        @Override
        public void startElement(String uri, String localName, String qName, Attributes attributes) {
            Attributes copy = new AttributesImpl(attributes);
            inBody |= "body".equals(localName);
            events.add(handler -> handler.startElement(uri, localName, qName, copy));
        }

        @Override
        public void endElement(String uri, String localName, String qName) {
            inBody &= !"body".equals(localName);
            events.add(handler -> handler.endElement(uri, localName, qName));
        }

        @Override
        public void characters(char[] ch, int start, int length) {
            String text = new String(ch, start, length);
            hasText |= inBody && !text.isBlank();
            events.add(handler -> handler.characters(text.toCharArray(), 0, text.length()));
        }

        @Override
        public void ignorableWhitespace(char[] ch, int start, int length) {
            String text = new String(ch, start, length);
            events.add(handler -> handler.ignorableWhitespace(text.toCharArray(), 0, text.length()));
        }

        void replayTo(ContentHandler handler) throws SAXException {
            for (SaxEvent event : events) {
                event.replay(handler);
            }
        }
    }

    private interface SaxEvent {
        void replay(ContentHandler handler) throws SAXException;
    }
}
