package org.icij.spewer;

import org.apache.tika.metadata.Metadata;
import org.icij.extract.document.DigestIdentifier;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.EmbeddedTikaDocument;
import org.icij.extract.document.TikaDocument;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.fest.assertions.Assertions.assertThat;

/**
 * The spew walk must close every document's reader (root and all embeds). Spilled embed readers are
 * open file handles, so leaving them open exhausts the process file-descriptor limit on large
 * containers (PST/mailbox with tens of thousands of items).
 */
public class SpewerClosesReadersTest {

    /** A reader that records whether it was closed. */
    private static class TrackingReader extends StringReader {
        boolean closed = false;
        TrackingReader(final String s) { super(s); }
        @Override public void close() { closed = true; super.close(); }
    }

    /** A spewer that reads each document's reader exactly as the real spewers do, without closing it. */
    private static class ReadingSpewer extends Spewer {
        ReadingSpewer() { super(new FieldNames()); }
        @Override protected void writeDocument(TikaDocument doc, TikaDocument parent, TikaDocument root, int level) throws IOException {
            toString(doc.getReader());
        }
        @Override public void close() {}
    }

    @Test
    public void test_write_closes_every_reader_in_the_tree() throws Exception {
        DocumentFactory factory = new DocumentFactory()
                .withIdentifier(new DigestIdentifier("SHA-256", StandardCharsets.UTF_8));
        TikaDocument root = factory.create("root", Paths.get("root"));

        List<TrackingReader> readers = new ArrayList<>();
        TrackingReader rootReader = new TrackingReader("root body");
        root.setReader(rootReader);
        readers.add(rootReader);

        // Two embeds, one of them nested, to cover the recursive walk.
        EmbeddedTikaDocument e1 = root.addEmbed(new Metadata());
        TrackingReader e1Reader = new TrackingReader("embed 1");
        e1.setReader(e1Reader);
        readers.add(e1Reader);

        EmbeddedTikaDocument e2 = root.addEmbed(new Metadata());
        TrackingReader e2Reader = new TrackingReader("embed 2");
        e2.setReader(e2Reader);
        readers.add(e2Reader);

        EmbeddedTikaDocument e1a = e1.addEmbed(new Metadata());
        TrackingReader e1aReader = new TrackingReader("embed 1a");
        e1a.setReader(e1aReader);
        readers.add(e1aReader);

        new ReadingSpewer().write(root);

        for (TrackingReader reader : readers) {
            assertThat(reader.closed).isTrue();
        }
    }
}
