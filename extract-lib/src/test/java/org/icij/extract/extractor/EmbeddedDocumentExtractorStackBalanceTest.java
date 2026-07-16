package org.icij.extract.extractor;

import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.parser.DigestingParser;
import org.apache.tika.parser.ParseContext;
import org.icij.extract.document.DigestIdentifier;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.TikaDocument;
import org.icij.extract.document.TikaDocumentSource;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.xml.sax.helpers.DefaultHandler;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

import static org.fest.assertions.Assertions.assertThat;
import static org.fest.assertions.Fail.fail;

/**
 * F4a: {@code DigestEmbeddedDocumentExtractor.delegateParsing} pushes the current embed onto
 * {@code documentStack} LATE (after digesting/documentCallback), but the {@code finally} block
 * always pops. If digest() throws before the push -- a per-message parse/digest failure, the
 * kind a resilient container isolates per-entry (see
 * {@link org.icij.extract.parser.ResilientOutlookPSTParser#emitMessage}, which catches and
 * continues to the next sibling) -- the {@code finally} still pops the parent this call never
 * pushed, underflowing the stack for every embed walked afterwards.
 */
public class EmbeddedDocumentExtractorStackBalanceTest {

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    private static final byte[] FAILING_CONTENT = "first message body".getBytes();
    private static final byte[] OK_CONTENT = "second message body".getBytes();

    private DocumentFactory documentFactory;
    private Path rootFile;

    @Before
    public void setUp() throws Exception {
        documentFactory = new DocumentFactory().withIdentifier(new DigestIdentifier("SHA-256", Charset.defaultCharset()));
        rootFile = tmp.newFile("root.bin").toPath();
        Files.write(rootFile, "root content".getBytes());
    }

    private static Metadata messageMetadata(String name) {
        Metadata metadata = new Metadata();
        metadata.set(TikaCoreProperties.RESOURCE_NAME_KEY, name);
        return metadata;
    }

    // A root TikaDocument only gets a resolvable getId() once its metadata has been digested;
    // normally that happens as a side effect of the outer DigestingParser parsing the root
    // document in EmbeddedDocumentExtractor.extract()/extractAll(). Since this test drives
    // delegateParsing directly (bypassing that outer parse), digest the root explicitly so
    // embed.getParent().getId() resolves instead of NPE-ing on a missing hash.
    private TikaDocument freshDigestedRoot(DigestingParser.Digester digester) throws Exception {
        TikaDocument root = documentFactory.create(rootFile);
        try (TikaInputStream tis = TikaInputStream.get(rootFile)) {
            digester.digest(tis, root.getMetadata(), new ParseContext());
        }
        return root;
    }

    @Test
    public void a_failed_message_before_the_stack_push_does_not_cascade_to_the_next_sibling() throws Exception {
        //GIVEN the id the second (valid) message would get in a normal walk, computed with a
        // real digester against a throwaway root instance pointing at the same file.
        UpdatableDigester realDigester = new UpdatableDigester("prj", "SHA-256");
        TikaDocument controlRoot = freshDigestedRoot(realDigester);
        EmbeddedDocumentExtractor.DigestEmbeddedDocumentMemoryExtractor control =
                new EmbeddedDocumentExtractor.DigestEmbeddedDocumentMemoryExtractor(
                        controlRoot, "unused-sentinel", new ParseContext(), realDigester, "SHA-256");
        control.delegateParsing(new ByteArrayInputStream(FAILING_CONTENT), new DefaultHandler(), messageMetadata("message-1.eml"));
        control.delegateParsing(new ByteArrayInputStream(OK_CONTENT), new DefaultHandler(), messageMetadata("message-2.eml"));
        String secondMessageId = controlRoot.getEmbeds().get(1).getId();

        //GIVEN a digester that throws on the FIRST digest() call only (simulating one bad
        // message a resilient container isolates -- catches, logs, moves to the next sibling)
        // and delegates to the real digester afterwards.
        AtomicInteger calls = new AtomicInteger();
        DigestingParser.Digester flakyDigester = (is, metadata, context) -> {
            if (calls.getAndIncrement() == 0) {
                throw new IOException("simulated per-message digest failure");
            }
            realDigester.digest(is, metadata, context);
        };
        TikaDocument testRoot = freshDigestedRoot(realDigester);
        EmbeddedDocumentExtractor.DigestEmbeddedDocumentMemoryExtractor extractor =
                new EmbeddedDocumentExtractor.DigestEmbeddedDocumentMemoryExtractor(
                        testRoot, secondMessageId, new ParseContext(), flakyDigester, "SHA-256");

        //WHEN the first message's digest fails and the caller isolates it, the way a resilient
        // container does
        try {
            extractor.delegateParsing(new ByteArrayInputStream(FAILING_CONTENT), new DefaultHandler(), messageMetadata("message-1.eml"));
            fail("expected the simulated digest failure to propagate to the caller");
        } catch (IOException expected) {
            // isolated by the caller; the walk must go on to the next sibling
        }

        //THEN the stack must not be corrupted: the second, valid sibling is still walked and
        // found, instead of the earlier failure underflowing the stack and breaking every
        // subsequent embed.
        extractor.delegateParsing(new ByteArrayInputStream(OK_CONTENT), new DefaultHandler(), messageMetadata("message-2.eml"));

        TikaDocumentSource found = extractor.getDocument();
        assertThat(found).isNotNull();
        assertThat(new String(found.getContent())).isEqualTo(new String(OK_CONTENT));
    }
}
