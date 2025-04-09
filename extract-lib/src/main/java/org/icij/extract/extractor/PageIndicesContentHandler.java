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
    /**
     * this is a hack to simulate the text.trim() that is done in ElasticsearchSpewer
     */
    private boolean firstCharReceived = false;
    private boolean firstPage = true;
    private long charIndex = 0;
    private long pageStartIndex = 0;
    private boolean startPageCalled = false;
    private boolean documentStarted;
    private boolean startDocumentCalled = false;

    private final List<Pair<Long, Long>> pageIndices = new LinkedList<>();

    public PageIndicesContentHandler(ContentHandler handler) {
        this(handler, true);
    }
    public PageIndicesContentHandler(ContentHandler handler, boolean notEmbedded) {
        super(handler);
        this.documentStarted = notEmbedded;
    }

    public List<Pair<Long, Long>> getPageIndices() {
        return pageIndices;
    }

    @Override
    public void startDocument() throws SAXException {
        super.startDocument();
        if (startDocumentCalled && !documentStarted) {
            documentStarted = true;
        }
        startDocumentCalled = true;
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
        super.startElement(uri, localName, qName, atts);
        if (pageTag.endsWith(qName) && pageClass.equals(atts.getValue("class"))) {
            startPage();
        }
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        super.characters(ch, start, length);
        firstCharReceived = documentStarted;
        if (documentStarted) {
            charIndex += length;
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
    public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
        super.ignorableWhitespace(ch, start, length);
        if (firstCharReceived && documentStarted) {
            charIndex += length;
        }
    }

    protected void startPage() {
        startPageCalled = true;
        if (firstPage) {
            firstPage = false;
        } else {
            pageStartIndex = charIndex;
        }
    }

    protected void endPage() {
        if (startPageCalled) {
            startPageCalled = false;
        } else if (!pageIndices.isEmpty()) {
            // endPage() is being called several times
            // so we are replacing the last page with additional characters
            pageIndices.remove(pageIndices.size() - 1);
        }
        if (charIndex != 0) {
            pageIndices.add(Pair.of(pageStartIndex, charIndex));
        }
    }
}
