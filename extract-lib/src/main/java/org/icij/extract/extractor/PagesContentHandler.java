package org.icij.extract.extractor;

import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.util.LinkedList;
import java.util.List;


/**
 * inspired by
 * <a href="https://github.com/mkalus/tika-page-extractor/blob/master/src/main/java/de/auxnet/PageContentHandler.java">...</a>
 */
public class PagesContentHandler extends PageIndicesContentHandler {
    private final StringBuffer currentPage = new StringBuffer();
    private final List<String> pages = new LinkedList<>();

    public PagesContentHandler(ContentHandler handler) {
        super(handler);
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        super.characters(ch, start, length);
        if (shouldCountChars()) {
            currentPage.append(ch, start, length);
        }
    }

    @Override
    public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
        super.ignorableWhitespace(ch, start, length);
        if (shouldCountChars()) {
            currentPage.append(ch, start, length);
        }
    }

    protected void startPage() {
        super.startPage();
        currentPage.setLength(0);
    }

    protected boolean endPage() {
        boolean startPageCalled = super.endPage();
        if (!startPageCalled && !pages.isEmpty()) {
            // endPage() is being called several times
            // so we are replacing the last page with additional characters
            pages.remove(pages.size() - 1);
        }
        if (charIndex != 0) {
            pages.add(currentPage.toString());
        }
        return startPageCalled;
    }

    public List<String> getPages() {return pages;}
}
