package org.icij.extract.parser;

import org.apache.tika.extractor.EmbeddedDocumentExtractor;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.junit.Test;
import org.xml.sax.ContentHandler;

import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.fest.assertions.Assertions.assertThat;

public class ResilientOutlookPSTParserTest {

    private Path testPst() throws Exception {
        return Paths.get(getClass().getResource("/documents/pst/testPST.pst").toURI());
    }

    // Counts how many embedded mail items the parser emits.
    private static class CountingExtractor implements EmbeddedDocumentExtractor {
        final List<String> mailFolders = new ArrayList<>();

        public boolean shouldParseEmbedded(Metadata metadata) {
            return true;
        }

        public void parseEmbedded(InputStream stream, ContentHandler handler, Metadata metadata, boolean outputHtml) {
            String contentType = metadata.get(org.apache.tika.metadata.TikaCoreProperties.CONTENT_TYPE_PARSER_OVERRIDE);
            if (contentType != null && contentType.contains("pst-mail-item")) {
                mailFolders.add(metadata.get(org.apache.tika.metadata.PST.PST_FOLDER_PATH));
            }
        }
    }

    @Test
    public void test_emits_every_message_and_sets_reconciliation_metadata() throws Exception {
        ResilientOutlookPSTParser parser = new ResilientOutlookPSTParser();
        CountingExtractor counting = new CountingExtractor();
        ParseContext context = new ParseContext();
        context.set(Parser.class, parser);
        context.set(EmbeddedDocumentExtractor.class, counting);
        Metadata metadata = new Metadata();

        try (InputStream is = TikaInputStream.get(testPst(), metadata)) {
            parser.parse(is, new BodyContentHandler(-1), metadata, context);
        }

        assertThat(counting.mailFolders).hasSize(7);
        assertThat(metadata.get(ResilientOutlookPSTParser.PST_EXPECTED)).isEqualTo("7");
        assertThat(metadata.get(ResilientOutlookPSTParser.PST_EMITTED)).isEqualTo("7");
        assertThat(metadata.get(ResilientOutlookPSTParser.PST_FAILED)).isEqualTo("0");
    }
}
