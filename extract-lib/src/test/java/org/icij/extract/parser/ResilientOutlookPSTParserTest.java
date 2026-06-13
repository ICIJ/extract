package org.icij.extract.parser;

import org.apache.tika.extractor.EmbeddedDocumentExtractor;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.PathIdentifier;
import org.icij.extract.document.TikaDocument;
import org.icij.extract.extractor.Extractor;
import org.junit.Test;
import org.xml.sax.ContentHandler;

import java.io.InputStream;
import java.io.Reader;
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

    private int countMailItems(TikaDocument doc) {
        int count = 0;
        for (TikaDocument embed : doc.getEmbeds()) {
            String contentType = embed.getMetadata().get(Metadata.CONTENT_TYPE);
            if (contentType != null && contentType.contains("pst-mail-item")) {
                count++;
            }
            count += countMailItems(embed);
        }
        return count;
    }

    private int countAllEmbeds(TikaDocument doc) {
        int count = 0;
        for (TikaDocument embed : doc.getEmbeds()) {
            count += 1 + countAllEmbeds(embed);
        }
        return count;
    }

    @Test
    public void test_extractor_pipeline_uses_resilient_parser() throws Exception {
        Extractor extractor = new Extractor(new DocumentFactory().withIdentifier(new PathIdentifier()));
        TikaDocument doc = extractor.extract(testPst());
        try (Reader reader = doc.getReader()) {
            char[] buffer = new char[8192];
            while (reader.read(buffer) != -1) {
                // drain the reader so embedded parsing completes
            }
        }
        // Reconciliation metadata is only set by ResilientOutlookPSTParser, never
        // by Tika's stock parser, so its presence proves the override is wired.
        assertThat(doc.getMetadata().get(ResilientOutlookPSTParser.PST_EXPECTED)).isEqualTo("7");
        assertThat(doc.getMetadata().get(ResilientOutlookPSTParser.PST_EMITTED)).isEqualTo("7");
        // 8, not 7: "FW: First email" contains a nested attached email which
        // PSTMailItemParser also tags as a pst-mail-item, and countMailItems
        // recurses. The reconciliation metadata (PST_EMITTED=7) is the count of
        // top-level messages our parser emits and is asserted above.
        assertThat(countMailItems(doc)).isEqualTo(8);
        // Attachment fidelity: testPST.pst has messages with attachments, so the
        // tree must hold strictly more nodes than the 7 mail items.
        assertThat(countAllEmbeds(doc)).isGreaterThan(7);
    }
}
