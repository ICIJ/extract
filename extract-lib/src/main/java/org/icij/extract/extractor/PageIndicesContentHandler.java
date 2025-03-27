package org.icij.extract.extractor;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tika.sax.ContentHandlerDecorator;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.util.LinkedList;
import java.util.List;



/**
 * inspired by
 * <a href="https://github.com/mkalus/tika-page-extractor/blob/master/src/main/java/de/auxnet/PageContentHandler.java">...</a>
 */
public class PageIndicesContentHandler extends ContentHandlerDecorator {
    final static private String pageTag = "div";
    final static private String pageClass = "page";
    private boolean firstPageStarted = false;
    private long charIndex = 0;
    private long pageStartIndex = 0;
    private boolean startPageCalled = false;

    private final List<Pair<Long, Long>> pageIndices = new LinkedList<>();

    public PageIndicesContentHandler(ContentHandler handler) {
        super(handler);
    }

    public List<Pair<Long, Long>> getPageIndices() {
        return pageIndices;
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
        super.startElement(uri, localName, qName, atts);
        if (pageTag.endsWith(qName) && pageClass.equals(atts.getValue("class"))) {
            startPage();
        }
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        super.endElement(uri, localName, qName);
        if (pageTag.endsWith(qName)) {
            endPage();
        }
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        super.characters(ch, start, length);
        if (firstPageStarted) {
            charIndex += length;
        }
    }

    @Override
    public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
        super.ignorableWhitespace(ch, start, length);
        if (firstPageStarted) {
            charIndex += length;
        }
    }

    protected void startPage() {
        firstPageStarted = true;
        startPageCalled = true;
        pageStartIndex = charIndex;
    }

    protected void endPage() {
        if (startPageCalled) {
            startPageCalled = false;
        } else if (!pageIndices.isEmpty()) {
            // endPage() is being called several times
            // so we are replacing the last page with additional characters
            pageIndices.remove(pageIndices.size() - 1);
        }
        pageIndices.add(Pair.of(pageStartIndex, charIndex));
    }
}
