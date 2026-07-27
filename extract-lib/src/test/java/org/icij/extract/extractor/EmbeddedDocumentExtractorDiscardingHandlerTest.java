package org.icij.extract.extractor;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.sax.XHTMLContentHandler;
import org.junit.Test;
import org.xml.sax.ContentHandler;

import static org.fest.assertions.Assertions.assertThat;

/**
 * The ARTIFACT/download walk needs each embed's bytes and digest, never the extracted text. A buffering
 * handler accumulates a whole document's text for nothing, and on a very large body overflows Java's
 * array limit, aborting the root document with an OutOfMemoryError no per-embed handler catches. That
 * scale is not reproducible in CI, so this pins the property instead: no retained body text.
 */
public class EmbeddedDocumentExtractorDiscardingHandlerTest {

    private static final String BODY_TEXT = "text the artifact walk never reads";

    @Test
    public void doesNotAccumulateBodyText() throws Exception {
        final ContentHandler handler = EmbeddedDocumentExtractor.discardingHandler();

        // BodyContentHandler forwards only /xhtml:html/xhtml:body/descendant::node(), so the text has
        // to arrive inside a real XHTML skeleton or this test would pass without proving anything.
        final XHTMLContentHandler xhtml = new XHTMLContentHandler(handler, new Metadata());
        xhtml.startDocument();
        xhtml.characters(BODY_TEXT);
        xhtml.endDocument();

        // RED with BodyContentHandler(-1), whose unbounded StringWriter hands the text back here.
        assertThat(handler.toString().contains(BODY_TEXT)).isFalse();
    }
}
