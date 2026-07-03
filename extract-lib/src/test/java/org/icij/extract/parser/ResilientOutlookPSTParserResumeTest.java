package org.icij.extract.parser;

import org.apache.tika.extractor.EmbeddedDocumentExtractor;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.icij.extract.document.DigestIdentifier;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.TikaDocument;
import org.icij.extract.extractor.Extractor;
import org.icij.spewer.FieldNames;
import org.icij.spewer.Spewer;
import org.icij.task.Options;
import org.junit.Test;
import org.xml.sax.ContentHandler;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.fest.assertions.Assertions.assertThat;

/**
 * Resumable-extraction seam (plan item 2.4). Proves that supplying a {@link ResumePolicy} skips the
 * mail messages a previous run already emitted, without re-parsing them and without perturbing the
 * ids of the messages that ARE emitted; and that the seam is inert by default.
 */
public class ResilientOutlookPSTParserResumeTest {

    private Path testPst() throws Exception {
        return Paths.get(getClass().getResource("/documents/pst/testPST.pst").toURI());
    }

    // Records, per emitted mail item, its stamped resume key. mailItems holds one entry per emitted
    // message (so its size is the number of messages actually parsed, not skipped).
    private static class ResumeRecordingExtractor implements EmbeddedDocumentExtractor {
        final List<String> resumeKeys = new ArrayList<>();

        public boolean shouldParseEmbedded(Metadata metadata) { return true; }

        public void parseEmbedded(InputStream stream, ContentHandler handler, Metadata metadata, boolean outputHtml) {
            String override = metadata.get(TikaCoreProperties.CONTENT_TYPE_PARSER_OVERRIDE);
            if (override != null && override.contains("pst-mail-item")) {
                resumeKeys.add(metadata.get(ResilientOutlookPSTParser.PST_RESUME_KEY));
            }
        }
    }

    private static Metadata parse(Path file, ResumePolicy policy, ResumeRecordingExtractor rec) throws Exception {
        ResilientOutlookPSTParser parser = new ResilientOutlookPSTParser();
        ParseContext context = new ParseContext();
        context.set(Parser.class, parser);
        context.set(EmbeddedDocumentExtractor.class, rec);
        if (policy != null) {
            context.set(ResumePolicy.class, policy);
        }
        Metadata metadata = new Metadata();
        try (InputStream is = TikaInputStream.get(file, metadata)) {
            parser.parse(is, new BodyContentHandler(-1), metadata, context);
        }
        return metadata;
    }

    @Test
    public void test_no_policy_emits_every_message_and_stamps_a_distinct_resume_key() throws Exception {
        ResumeRecordingExtractor rec = new ResumeRecordingExtractor();
        Metadata metadata = parse(testPst(), null, rec);

        // All 7 messages parsed; no resume happened.
        assertThat(rec.resumeKeys).hasSize(7);
        assertThat(metadata.get(ResilientOutlookPSTParser.PST_EMITTED)).isEqualTo("7");
        assertThat(metadata.get(ResilientOutlookPSTParser.PST_RESUMED_SKIPPED)).isNull();
        // Every emitted message carries a distinct, parseable resume key.
        Set<Long> distinct = new HashSet<>();
        for (String key : rec.resumeKeys) {
            assertThat(key).isNotNull();
            distinct.add(Long.parseLong(key));
        }
        assertThat(distinct).hasSize(7);
    }

    @Test
    public void test_policy_skipping_all_units_parses_nothing_but_reconciles_honestly() throws Exception {
        ResumeRecordingExtractor rec = new ResumeRecordingExtractor();
        Metadata metadata = parse(testPst(), key -> true, rec);

        // No message was parsed/emitted...
        assertThat(rec.resumeKeys).isEmpty();
        // ...yet reconciliation still accounts for all 7 (they are in the index from a previous run),
        // so a resumed run does not read as catastrophic loss.
        assertThat(metadata.get(ResilientOutlookPSTParser.PST_EXPECTED)).isEqualTo("7");
        assertThat(metadata.get(ResilientOutlookPSTParser.PST_EMITTED)).isEqualTo("7");
        assertThat(metadata.get(ResilientOutlookPSTParser.PST_FAILED)).isEqualTo("0");
        assertThat(metadata.get(ResilientOutlookPSTParser.PST_RESUMED_SKIPPED)).isEqualTo("7");
    }

    @Test
    public void test_policy_skipping_a_subset_emits_exactly_the_complement() throws Exception {
        // First pass: learn the stable resume keys stamped on each message.
        ResumeRecordingExtractor firstPass = new ResumeRecordingExtractor();
        parse(testPst(), null, firstPass);
        List<Long> allKeys = new ArrayList<>();
        for (String k : firstPass.resumeKeys) {
            allKeys.add(Long.parseLong(k));
        }
        assertThat(allKeys).hasSize(7);

        // Mark the first three as already done by a previous run.
        Set<Long> done = new HashSet<>(allKeys.subList(0, 3));
        assertThat(done).hasSize(3);

        // Second pass: only the complement (4 messages) must be parsed, and none of them may be a
        // done key.
        ResumeRecordingExtractor resumed = new ResumeRecordingExtractor();
        Metadata metadata = parse(testPst(), done::contains, resumed);

        assertThat(resumed.resumeKeys).hasSize(4);
        for (String k : resumed.resumeKeys) {
            assertThat(done).excludes(Long.parseLong(k));
        }
        assertThat(metadata.get(ResilientOutlookPSTParser.PST_EMITTED)).isEqualTo("7");
        assertThat(metadata.get(ResilientOutlookPSTParser.PST_RESUMED_SKIPPED)).isEqualTo("3");
        assertThat(metadata.get(ResilientOutlookPSTParser.PST_FAILED)).isEqualTo("0");
    }

    // ---- End-to-end id determinism through Extractor (fan-out on, OCR off) ------------------------

    // Thread-safe recorder of every written document's id, plus the resume key of message docs.
    private static class IdRecordingSpewer extends Spewer {
        final List<String> allIds = new CopyOnWriteArrayList<>();
        // resumeKey -> the message document's own id (message docs only).
        final Map<Long, String> messageIdByResumeKey = new java.util.concurrent.ConcurrentHashMap<>();

        IdRecordingSpewer() { super(new FieldNames()); }

        @Override
        protected void writeDocument(TikaDocument doc, TikaDocument parent, TikaDocument root, int level) throws IOException {
            try { Spewer.toString(doc.getReader()); } catch (Exception ignored) { /* drive the parse */ }
            allIds.add(doc.getId());
            String key = doc.getMetadata().get(ResilientOutlookPSTParser.PST_RESUME_KEY);
            if (key != null) {
                messageIdByResumeKey.put(Long.parseLong(key), doc.getId());
            }
        }
    }

    private IdRecordingSpewer extractWith(ResumePolicy policy) throws Exception {
        IdRecordingSpewer spewer = new IdRecordingSpewer();
        // A DigestIdentifier (as datashare uses) gives embeds content-derived ids; the default
        // PathIdentifier would return the root path for every embed, defeating the determinism check.
        final DocumentFactory factory = new DocumentFactory()
                .withIdentifier(new DigestIdentifier("SHA256", java.nio.charset.StandardCharsets.UTF_8));
        try (Extractor extractor = new Extractor(factory, Options.from(Map.of(
                "ocr", "false", "progressHeartbeatInterval", "0")))) {
            if (policy != null) {
                extractor.setResumePolicyProvider(p -> policy);
            }
            extractor.extract(testPst(), spewer);
        }
        return spewer;
    }

    @Test
    public void test_resumed_run_ids_are_a_strict_subset_of_the_full_run_ids() throws Exception {
        // Full run: capture every emitted id and the message ids by resume key.
        IdRecordingSpewer full = extractWith(null);
        assertThat(full.messageIdByResumeKey).hasSize(7);

        // Skip two messages on the resumed run.
        List<Long> keys = new ArrayList<>(full.messageIdByResumeKey.keySet());
        Collections.sort(keys);
        Set<Long> done = new HashSet<>(keys.subList(0, 2));
        IdRecordingSpewer resumed = extractWith(done::contains);

        Set<String> fullIds = new HashSet<>(full.allIds);
        Set<String> resumedIds = new HashSet<>(resumed.allIds);

        // Determinism: skipping perturbs NO surviving id. Every id written on the resumed run was
        // also written on the full run (subset), and the resumed run wrote strictly fewer documents.
        assertThat(fullIds.containsAll(resumedIds)).isTrue();
        assertThat(resumedIds.size()).isLessThan(fullIds.size());

        // The two skipped messages' own ids are absent from the resumed run.
        for (Long skippedKey : done) {
            assertThat(resumedIds).excludes(full.messageIdByResumeKey.get(skippedKey));
        }
        // The surviving five message ids are present in both runs, byte-identical.
        for (Long key : keys) {
            if (!done.contains(key)) {
                assertThat(resumedIds).contains(full.messageIdByResumeKey.get(key));
            }
        }
    }
}
