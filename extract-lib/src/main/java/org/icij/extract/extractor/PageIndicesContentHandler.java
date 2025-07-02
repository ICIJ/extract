package org.icij.extract.extractor;

import org.apache.tika.Tika;
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
    private long pageStartIndex = 0;
    private boolean bodyStarted = false;
    private int embeddedLevel = -1;

    protected boolean startPageCalled = false;
    protected long charIndex = 0;

    /**
     * this is a hack to simulate the text.trim() that is done in ElasticsearchSpewer
     */
    private boolean firstCharReceived = false;
    private final List<Pair<Long, Long>> pageIndices = new LinkedList<>();

    public PageIndicesContentHandler(ContentHandler handler) {
        super(handler);
    }

    public PageIndices getPageIndices() {
        return new PageIndices(Tika.getString(), pageIndices);
    }

    @Override
    public void startDocument() throws SAXException {
        super.startDocument();
        embeddedLevel++;
    }

    @Override
    public void endDocument() throws SAXException {
        super.endDocument();
        embeddedLevel--;
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
        super.startElement(uri, localName, qName, atts);
        if ("body".equals(qName)) {
            bodyStarted = true;
        }
        if (pageTag.endsWith(qName) && pageClass.equals(atts.getValue("class")) && embeddedLevel == 0) {
            startPage();
        }
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        super.characters(ch, start, length);
        firstCharReceived = documentStarted();
        if (shouldCountChars()) {
            charIndex += length;
        }
    }

    protected boolean shouldCountChars() {
        return documentStarted() && bodyStarted;
    }

    protected boolean documentStarted() {
        return embeddedLevel >= 0;
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        super.endElement(uri, localName, qName);
        if ("body".equals(qName)) {
            bodyStarted = false;
        }
        if (pageTag.endsWith(qName) && embeddedLevel == 0) {
            endPage();
        }
    }

    @Override
    public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
        super.ignorableWhitespace(ch, start, length);
        if (firstCharReceived && shouldCountChars()) {
            charIndex += length;
        }
    }

    protected void startPage() {
        startPageCalled = true;
        pageStartIndex = charIndex;
    }

    /**
     * endPage() called when end page div is found.
     * @return true if the page was declared started before: endPage() can be called several times
     */
    protected boolean endPage() {
        boolean memoizedStartPageCalled = startPageCalled;
        if (startPageCalled) {
            startPageCalled = false;
        } else if (!pageIndices.isEmpty()) {
            // endPage() is being called several times
            // so we are replacing the last page with additional characters
            pageIndices.remove(pageIndices.size() - 1);
        }
        if (charIndex != 0) {
            pageIndices.add(new Pair<>(pageStartIndex, charIndex));
        }
        return memoizedStartPageCalled;
    }
}
