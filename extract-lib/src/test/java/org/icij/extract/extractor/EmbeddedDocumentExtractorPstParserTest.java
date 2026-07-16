package org.icij.extract.extractor;

import org.apache.tika.parser.CompositeParser;
import org.apache.tika.parser.DigestingParser;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.ParserDecorator;
import org.apache.tika.parser.microsoft.pst.OutlookPSTParser;
import org.icij.extract.parser.ResilientOutlookPSTParser;
import org.junit.Test;

import java.nio.file.Paths;

import static org.fest.assertions.Assertions.assertThat;

/**
 * Verifies that {@link EmbeddedDocumentExtractor} -- the parser used by the ARTIFACT/download
 * re-extraction path -- wires in the resilient Outlook PST parser instead of Tika's stock one,
 * the same replacement {@link Extractor} applies at INDEX time. Without it, retrieval of any
 * document embedded in a PST/OST fails with Tika's stock parser (e.g. "OST 2013 support not
 * added yet"), even though the very same file indexed fine.
 */
public class EmbeddedDocumentExtractorPstParserTest {

    @Test
    public void test_parser_replaces_stock_outlook_pst_parser_without_ocr() {
        //GIVEN an extractor built the same way ARTIFACT/download re-extraction builds it (no OCR)
        EmbeddedDocumentExtractor extractor = new EmbeddedDocumentExtractor(
                new UpdatableDigester("prj", "SHA-256"), "SHA-256", Paths.get("/tmp"), false);

        //WHEN/THEN
        assertResilientOutlookParserIsWired(extractor);
    }

    @Test
    public void test_parser_replaces_stock_outlook_pst_parser_with_ocr() {
        //GIVEN an extractor built the same way ARTIFACT/download re-extraction builds it (with OCR)
        EmbeddedDocumentExtractor extractor = new EmbeddedDocumentExtractor(
                new UpdatableDigester("prj", "SHA-256"), "SHA-256", Paths.get("/tmp"), true);

        //WHEN/THEN
        assertResilientOutlookParserIsWired(extractor);
    }

    private void assertResilientOutlookParserIsWired(EmbeddedDocumentExtractor extractor) {
        Parser wrapped = extractor.getParser();
        assertThat(wrapped).isInstanceOf(DigestingParser.class);
        Parser base = ((ParserDecorator) wrapped).getWrappedParser();
        assertThat(base).isInstanceOf(CompositeParser.class);

        boolean hasResilientParser = Extractor.getAllSubParsers((CompositeParser) base)
                .anyMatch(ResilientOutlookPSTParser.class::isInstance);
        boolean hasStockParser = Extractor.getAllSubParsers((CompositeParser) base)
                .anyMatch(p -> p.getClass().equals(OutlookPSTParser.class));

        assertThat(hasResilientParser).isTrue();
        assertThat(hasStockParser).isFalse();
    }
}
