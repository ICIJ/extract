package org.icij.extract.extractor;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.sax.XHTMLContentHandler;
import org.junit.Test;
import org.xml.sax.ContentHandler;

import static org.fest.assertions.Assertions.assertThat;

/**
 * The ARTIFACT/download walk needs each embed's bytes and digest, never the extracted text: bytes are
 * written by documentCallback via Files.copy. A buffering handler therefore accumulates a whole
 * document's text for nothing, and on a very large body it overflows Java's array limit and aborts the
 * entire root document with an OutOfMemoryError that the per-embed Exception | LinkageError handlers
 * cannot catch. That scale is not reproducible in CI, so this pins the property instead: the handler
 * the walk parses into must not retain body text.
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
