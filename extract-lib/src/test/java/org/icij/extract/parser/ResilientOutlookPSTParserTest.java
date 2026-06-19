package org.icij.extract.parser;

import com.pff.PSTFile;
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

    // Resolves the OST 2013 path from -Dost2013.path (preferred) or the
    // OST2013_PATH env var (fallback, in case Surefire does not forward the
    // system property to the forked JVM). Null/blank means "not provided".
    private static String ost2013Path() {
        String p = System.getProperty("ost2013.path");
        if (p == null || p.isEmpty()) {
            p = System.getenv("OST2013_PATH");
        }
        return p;
    }

    // Opens the file, reads the PSTFileType, then closes the file handle
    // (guarding against a null handle so the close is safe in all cases).
    private static int pstFileType(Path file) throws Exception {
        PSTFile probe = new PSTFile(file.toString());
        try {
            return probe.getPSTFileType();
        } finally {
            if (probe.getFileHandle() != null) {
                probe.getFileHandle().close();
            }
        }
    }

    // Holder for the result of a full parse with a CountingExtractor.
    private static class ParseResult {
        final Metadata metadata;
        final CountingExtractor counting;

        ParseResult(Metadata metadata, CountingExtractor counting) {
            this.metadata = metadata;
            this.counting = counting;
        }
    }

    // Runs the parser over the given file with a CountingExtractor and returns
    // both the populated Metadata and the CountingExtractor so callers can make
    // assertions against either.
    private static ParseResult parseWithCounting(Path file) throws Exception {
        ResilientOutlookPSTParser parser = new ResilientOutlookPSTParser();
        CountingExtractor counting = new CountingExtractor();
        ParseContext context = new ParseContext();
        context.set(Parser.class, parser);
        context.set(EmbeddedDocumentExtractor.class, counting);
        Metadata metadata = new Metadata();

        try (InputStream is = TikaInputStream.get(file, metadata)) {
            parser.parse(is, new BodyContentHandler(-1), metadata, context);
        }
        return new ParseResult(metadata, counting);
    }

    // Drives the parser over a real type-36 (.ost) file with a counting
    // extractor and asserts zero-loss traversal: PST_FAILED must be "0"
    // (the real zero-loss guarantee), and the emitted count must be numeric
    // and >= the descriptor ground-truth (PST_EXPECTED). Emitted may exceed
    // expected when orphan recovery surfaces messages beyond enumerated
    // descriptors — that is a valid NO-LOSS outcome. Body bytes are
    // deliberately not asserted; that path depends on downstream deps
    // documented in the spec.
    private void assertFullyTraversedAsOst2013(Path ost) throws Exception {
        assertThat(pstFileType(ost)).isEqualTo(PSTFile.PST_TYPE_2013_UNICODE);

        ParseResult result = parseWithCounting(ost);
        Metadata metadata = result.metadata;
        CountingExtractor counting = result.counting;

        String expected = metadata.get(ResilientOutlookPSTParser.PST_EXPECTED);
        String emitted = metadata.get(ResilientOutlookPSTParser.PST_EMITTED);

        // Assert zero-loss first with a descriptive message so that the
        // unmeasurable path ("unknown") produces a clear failure rather than a
        // bare NumberFormatException from parseInt below.
        assertThat(metadata.get(ResilientOutlookPSTParser.PST_FAILED))
                .as("descriptor enumeration must succeed for this fixture; else loss is undetectable")
                .isEqualTo("0");

        // Only parse integers after confirming the measured path (PST_FAILED=="0"
        // implies PST_EXPECTED is numeric, not "unknown").
        int emittedCount = Integer.parseInt(emitted);
        assertThat(emittedCount).isGreaterThan(0);
        assertThat(emittedCount).isGreaterThanOrEqualTo(Integer.parseInt(expected));
        assertThat(counting.mailFolders.size()).isEqualTo(emittedCount);
    }

    // Runs only when a real type-36 .ost path is provided; otherwise skipped.
    // Run locally with:
    //   mvn -pl extract-lib test -Dtest=ResilientOutlookPSTParserTest#test_ost_2013_file_is_fully_traversed \
    //       -Dost2013.path="/abs/path/to/file.ost"
    @Test
    public void test_ost_2013_file_is_fully_traversed() throws Exception {
        String path = ost2013Path();
        org.junit.Assume.assumeTrue("set -Dost2013.path (or OST2013_PATH) to run", path != null && !path.isEmpty());
        assertFullyTraversedAsOst2013(Paths.get(path));
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

    // Fails the first message the parser attempts to emit (identified by its
    // resource name), on every attempt, and counts the rest. Proves one failing
    // message does not abort the remaining messages (no cascade) and that the
    // reconciliation count reflects exactly the injected failure.
    private static class FailFirstExtractor implements EmbeddedDocumentExtractor {
        private String poisoned = null;
        int emittedOk = 0;
        final java.util.List<String> attempts = new java.util.ArrayList<>();

        public boolean shouldParseEmbedded(org.apache.tika.metadata.Metadata metadata) {
            return true;
        }

        public void parseEmbedded(java.io.InputStream stream, org.xml.sax.ContentHandler handler,
                                  org.apache.tika.metadata.Metadata metadata, boolean outputHtml) throws java.io.IOException {
            String name = metadata.get(org.apache.tika.metadata.TikaCoreProperties.RESOURCE_NAME_KEY);
            attempts.add(name);
            if (poisoned == null) {
                poisoned = name;
            }
            if (name != null && name.equals(poisoned)) {
                throw new java.io.IOException("injected failure for " + name);
            }
            emittedOk++;
        }
    }

    @Test
    public void test_one_failing_message_does_not_abort_the_rest() throws Exception {
        ResilientOutlookPSTParser parser = new ResilientOutlookPSTParser();
        FailFirstExtractor failing = new FailFirstExtractor();
        ParseContext context = new ParseContext();
        context.set(Parser.class, parser);
        context.set(EmbeddedDocumentExtractor.class, failing);
        Metadata metadata = new Metadata();

        try (InputStream is = TikaInputStream.get(testPst(), metadata)) {
            parser.parse(is, new BodyContentHandler(-1), metadata, context);
        }

        // The parse completed (no cascade abort) and the six non-poisoned messages
        // were still emitted despite the first one failing on every attempt.
        assertThat(failing.emittedOk).isEqualTo(6);
        assertThat(metadata.get(ResilientOutlookPSTParser.PST_EXPECTED)).isEqualTo("7");
        assertThat(metadata.get(ResilientOutlookPSTParser.PST_EMITTED)).isEqualTo("6");
        assertThat(metadata.get(ResilientOutlookPSTParser.PST_FAILED)).isEqualTo("1");
    }

    // Throws a LinkageError (an Error, not an Exception) on the first message,
    // every attempt. Reproduces the gzip IllegalAccessError class of failure that
    // previously cascaded and aborted the whole PST.
    private static class FailFirstWithErrorExtractor implements EmbeddedDocumentExtractor {
        private String poisoned = null;
        int emittedOk = 0;

        public boolean shouldParseEmbedded(org.apache.tika.metadata.Metadata metadata) {
            return true;
        }

        public void parseEmbedded(java.io.InputStream stream, org.xml.sax.ContentHandler handler,
                                  org.apache.tika.metadata.Metadata metadata, boolean outputHtml) {
            String name = metadata.get(org.apache.tika.metadata.TikaCoreProperties.RESOURCE_NAME_KEY);
            if (poisoned == null) {
                poisoned = name;
            }
            if (name != null && name.equals(poisoned)) {
                throw new NoClassDefFoundError("injected linkage error for " + name);
            }
            emittedOk++;
        }
    }

    @Test
    public void test_linkage_error_on_one_message_does_not_abort_the_rest() throws Exception {
        ResilientOutlookPSTParser parser = new ResilientOutlookPSTParser();
        FailFirstWithErrorExtractor failing = new FailFirstWithErrorExtractor();
        ParseContext context = new ParseContext();
        context.set(Parser.class, parser);
        context.set(EmbeddedDocumentExtractor.class, failing);
        Metadata metadata = new Metadata();

        try (InputStream is = TikaInputStream.get(testPst(), metadata)) {
            parser.parse(is, new BodyContentHandler(-1), metadata, context);
        }

        // A LinkageError on one message is isolated; the other six still emit.
        assertThat(failing.emittedOk).isEqualTo(6);
        assertThat(metadata.get(ResilientOutlookPSTParser.PST_EXPECTED)).isEqualTo("7");
        assertThat(metadata.get(ResilientOutlookPSTParser.PST_EMITTED)).isEqualTo("6");
        assertThat(metadata.get(ResilientOutlookPSTParser.PST_FAILED)).isEqualTo("1");
    }

    @Test
    public void test_emits_every_message_and_sets_reconciliation_metadata() throws Exception {
        ParseResult result = parseWithCounting(testPst());

        assertThat(result.counting.mailFolders).hasSize(7);
        assertThat(result.metadata.get(ResilientOutlookPSTParser.PST_EXPECTED)).isEqualTo("7");
        assertThat(result.metadata.get(ResilientOutlookPSTParser.PST_EMITTED)).isEqualTo("7");
        assertThat(result.metadata.get(ResilientOutlookPSTParser.PST_FAILED)).isEqualTo("0");
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
