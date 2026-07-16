package org.icij.extract.extractor;

import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.icij.extract.document.DigestIdentifier;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.EmbeddedTikaDocument;
import org.icij.extract.document.TikaDocument;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.xml.sax.ContentHandler;
import org.xml.sax.helpers.DefaultHandler;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Set;

import static org.fest.assertions.Assertions.assertThat;
import static org.fest.assertions.Fail.fail;

/**
 * OST-4b: an embed whose bytes cannot be read (an OST-2013 multi-block TRUNCATED by-value
 * attachment throws {@link IndexOutOfBoundsException} at {@code com.pff.PSTNodeInputStream.read}
 * during the retrieval walk's per-embed spool) must NOT leave that embed's id with no raw.
 * The index (EmbedSpawner) keeps such an embed and composes a CONTENT-LESS id for it
 * ({@link DigestIdentifier} null-hash path), then polls it; if the retrieval walk lets the read
 * failure propagate past the write, {@code RawArtifact} reports "produced no bytes" (F4b).
 *
 * The retrieval walk therefore mirrors EmbedSpawner's per-embed resilience: on a read/parse
 * failure it writes a zero-byte raw under the SAME id (embed.getId() with no content hash ==
 * the indexed id) before letting the exception propagate, so Tika's per-attachment isolation
 * continues the walk to the next sibling exactly as before, only now every visited embed's id
 * resolves. Cancellation (an interrupt) is never swallowed.
 */
public class EmbeddedDocumentExtractorUnreadableEmbedTest {

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    private DocumentFactory documentFactory;
    private Path rootFile;

    @Before
    public void setUp() throws Exception {
        documentFactory = new DocumentFactory().withIdentifier(new DigestIdentifier("SHA-256", Charset.defaultCharset()));
        rootFile = tmp.newFile("root.bin").toPath();
        Files.write(rootFile, "root content".getBytes());
    }

    private static Metadata named(String name) {
        Metadata metadata = new Metadata();
        metadata.set(TikaCoreProperties.RESOURCE_NAME_KEY, name);
        return metadata;
    }

    // A root only has a resolvable getId() once its metadata is digested; the direct-drive tests
    // below bypass the outer parse that normally does this, so digest the root explicitly (mirrors
    // EmbeddedDocumentExtractorStackBalanceTest.freshDigestedRoot).
    private TikaDocument freshDigestedRoot(UpdatableDigester digester) throws Exception {
        TikaDocument root = documentFactory.create(rootFile);
        try (TikaInputStream tis = TikaInputStream.get(rootFile)) {
            digester.digest(tis, root.getMetadata(), new ParseContext());
        }
        return root;
    }

    // Reading this stream throws IndexOutOfBoundsException on first read, reproducing the
    // com.pff.PSTNodeInputStream.read defect on a multi-block truncated by-value attachment.
    private static InputStream unreadableStream() {
        return new InputStream() {
            @Override
            public int read() {
                throw new IndexOutOfBoundsException("Index: 5, Size: 5");
            }

            @Override
            public int read(byte[] b, int off, int len) {
                throw new IndexOutOfBoundsException("Index: 5, Size: 5");
            }
        };
    }

    @Test
    public void unreadable_embed_writes_content_less_raw_and_siblings_survive() throws Exception {
        //GIVEN the extractAll write-all extractor over an artifact dir
        Path artifactDir = tmp.newFolder("artifacts").toPath();
        UpdatableDigester digester = new UpdatableDigester("prj", "SHA-256");
        TikaDocument root = freshDigestedRoot(digester);
        EmbeddedDocumentExtractor.DigestAllEmbeddedDocumentExtractor extractor =
                new EmbeddedDocumentExtractor.DigestAllEmbeddedDocumentExtractor(
                        root, new ParseContext(), digester, "SHA-256", artifactDir);

        //WHEN a healthy sibling, then an unreadable embed, then another healthy sibling are walked
        extractor.delegateParsing(new ByteArrayInputStream("good-1".getBytes()), new DefaultHandler(), named("good1.txt"));
        try {
            extractor.delegateParsing(unreadableStream(), new DefaultHandler(), named("bad.jpg"));
            fail("expected the read failure to propagate to the caller's per-attachment isolation");
        } catch (RuntimeException isolated) {
            // Isolated by the caller, exactly like Tika does around each attachment at index/retrieval.
            assertThat(isolated).isInstanceOf(IndexOutOfBoundsException.class);
        }
        extractor.delegateParsing(new ByteArrayInputStream("good-2".getBytes()), new DefaultHandler(), named("good2.txt"));

        //THEN (1) the failing embed still gets a zero-byte raw under the content-less id the index composes
        assertThat(root.getEmbeds()).hasSize(3);
        EmbeddedTikaDocument bad = root.getEmbeds().get(1);
        assertThat(bad.getHash()).isNull(); // no file digest -> content-less id (DigestIdentifier null-hash path)
        Path badRaw = EmbeddedDocumentExtractor.getEmbeddedPath(artifactDir, bad.getId());
        assertThat(badRaw.toFile()).isFile();
        assertThat(Files.size(badRaw)).isEqualTo(0L);

        //AND (2) both sibling embeds are still written with their real bytes
        Path good1Raw = EmbeddedDocumentExtractor.getEmbeddedPath(artifactDir, root.getEmbeds().get(0).getId());
        Path good2Raw = EmbeddedDocumentExtractor.getEmbeddedPath(artifactDir, root.getEmbeds().get(2).getId());
        assertThat(good1Raw.toFile()).isFile();
        assertThat(good2Raw.toFile()).isFile();
        assertThat(Files.size(good1Raw)).isGreaterThan(0L);
        assertThat(Files.size(good2Raw)).isGreaterThan(0L);
        //AND (3) the walk continued past the failure without cascading (the third call did not throw)
    }

    // A parser that always throws a non-Tika RuntimeException, reproducing a sub-parse failure that
    // hits AFTER the entry has been successfully spooled + digested (unlike the read failure above,
    // which throws while spooling). EmbedParser.delegateParsing only catches Tika exceptions, so this
    // propagates into DigestEmbeddedDocumentExtractor's per-embed catch.
    private static Parser subParseFailingParser() {
        return new Parser() {
            @Override
            public Set<MediaType> getSupportedTypes(ParseContext context) {
                return Collections.emptySet();
            }

            @Override
            public void parse(InputStream stream, ContentHandler handler, Metadata metadata, ParseContext context) {
                throw new IllegalStateException("sub-parse boom");
            }
        };
    }

    @Test
    public void spooled_embed_with_failing_subparse_writes_real_bytes_under_content_full_id() throws Exception {
        //GIVEN a write-all extractor whose delegate parser throws only during the sub-parse, so the
        //embed's bytes are spooled and digested OK before the failure (id is CONTENT-FULL)
        Path artifactDir = tmp.newFolder("artifacts").toPath();
        UpdatableDigester digester = new UpdatableDigester("prj", "SHA-256");
        TikaDocument root = freshDigestedRoot(digester);
        ParseContext context = new ParseContext();
        context.set(Parser.class, subParseFailingParser());
        EmbeddedDocumentExtractor.DigestAllEmbeddedDocumentExtractor extractor =
                new EmbeddedDocumentExtractor.DigestAllEmbeddedDocumentExtractor(
                        root, context, digester, "SHA-256", artifactDir);

        //WHEN an embed with real, readable bytes is walked and its sub-parse throws
        byte[] realBytes = "real-embedded-content".getBytes();
        try {
            extractor.delegateParsing(new ByteArrayInputStream(realBytes), new DefaultHandler(), named("has-bytes.bin"));
            fail("expected the sub-parse failure to propagate to the caller's per-attachment isolation");
        } catch (RuntimeException isolated) {
            assertThat(isolated).isInstanceOf(IllegalStateException.class);
        }

        //THEN the embed's REAL bytes are written under its content-full id (NOT an empty raw)
        assertThat(root.getEmbeds()).hasSize(1);
        EmbeddedTikaDocument embed = root.getEmbeds().get(0);
        assertThat(embed.getHash()).isNotNull(); // digest ran -> content-full id (contrast the read-fail case)
        Path raw = EmbeddedDocumentExtractor.getEmbeddedPath(artifactDir, embed.getId());
        assertThat(raw.toFile()).isFile();
        assertThat(Files.readAllBytes(raw)).isEqualTo(realBytes);
    }

    @Test
    public void interrupt_propagates_and_writes_no_content_less_raw() throws Exception {
        //GIVEN the same write-all extractor and an interrupted walk thread (cooperative cancellation)
        Path artifactDir = tmp.newFolder("artifacts").toPath();
        UpdatableDigester digester = new UpdatableDigester("prj", "SHA-256");
        TikaDocument root = freshDigestedRoot(digester);
        EmbeddedDocumentExtractor.DigestAllEmbeddedDocumentExtractor extractor =
                new EmbeddedDocumentExtractor.DigestAllEmbeddedDocumentExtractor(
                        root, new ParseContext(), digester, "SHA-256", artifactDir);

        //WHEN the thread is interrupted before an embed is parsed
        Thread.currentThread().interrupt();
        try {
            extractor.delegateParsing(new ByteArrayInputStream("x".getBytes()), new DefaultHandler(), named("cancelled.txt"));
            fail("expected InterruptedIOException to propagate (cancellation must not be swallowed)");
        } catch (InterruptedIOException expected) {
            // (4) cancellation aborts and is NOT turned into a content-less write
        } finally {
            Thread.interrupted(); // clear the flag so it never leaks to another test
        }

        //THEN no raw was written for the cancelled embed
        assertThat(root.getEmbeds()).hasSize(1);
        Path cancelledRaw = EmbeddedDocumentExtractor.getEmbeddedPath(artifactDir, root.getEmbeds().get(0).getId());
        assertThat(cancelledRaw.toFile().exists()).isFalse();
    }
}
