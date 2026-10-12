package org.icij.extract.ocr;

import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;
import org.xml.sax.helpers.DefaultHandler;

import java.util.ArrayList;
import java.util.List;

/**
 * Buffers SAX events so a read can be judged before it reaches the caller's handler. Only body text counts
 * as text: the head holds the document title, which is not what tesseract read.
 */
class SaxRecorder extends DefaultHandler {
    private final List<SaxEvent> events = new ArrayList<>();
    private boolean isInBody;
    private boolean hasText;

    boolean hasText() {
        return hasText;
    }

    void replayTo(ContentHandler handler) throws SAXException {
        for (SaxEvent event : events) {
            event.replay(handler);
        }
    }

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
        isInBody |= "body".equals(localName);
        events.add(handler -> handler.startElement(uri, localName, qName, copy));
    }

    @Override
    public void endElement(String uri, String localName, String qName) {
        isInBody &= !"body".equals(localName);
        events.add(handler -> handler.endElement(uri, localName, qName));
    }

    @Override
    public void characters(char[] ch, int start, int length) {
        String text = new String(ch, start, length);
        hasText |= isInBody && !text.isBlank();
        events.add(handler -> handler.characters(text.toCharArray(), 0, text.length()));
    }

    @Override
    public void ignorableWhitespace(char[] ch, int start, int length) {
        String text = new String(ch, start, length);
        events.add(handler -> handler.ignorableWhitespace(text.toCharArray(), 0, text.length()));
    }

    private interface SaxEvent {
        void replay(ContentHandler handler) throws SAXException;
    }
}
