package org.icij.extract.extractor;

import org.apache.tika.io.TemporaryResources;
import org.apache.tika.parser.ParseContext;
import org.icij.extract.document.DigestIdentifier;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.TikaDocument;
import org.junit.Test;

import java.nio.charset.Charset;
import java.nio.file.Paths;

import static org.fest.assertions.Assertions.assertThat;

public class EmbedSpawnerForkTest {

    private EmbedSpawner newSerialSpawner() {
        final TikaDocument root = new DocumentFactory()
                .withIdentifier(new DigestIdentifier("SHA-256", Charset.defaultCharset()))
                .create(Paths.get("/tmp/fake-root.ost"));
        // Serial constructor: no fan-out, no OCR. Sufficient to exercise fork()'s field sharing.
        return new EmbedSpawner(root, new ParseContext(), null,
                w -> new org.apache.tika.sax.BodyContentHandler(w),
                64L * 1024 * 1024, new TemporaryResources(), () -> false);
    }

    @Test
    public void testForkSharesBudgetButNotStack() {
        final EmbedSpawner base = newSerialSpawner();
        final EmbedSpawner forked = base.fork();

        // Same shared budget instance: a reservation on one is visible to the other.
        assertThat(forked.reservedBudget()).isSameAs(base.reservedBudget());

        // Independent DFS stacks: pushing on the fork must not change the base's stack depth.
        assertThat(forked.stackDepth()).isEqualTo(1); // seeded with root only
        assertThat(base.stackDepth()).isEqualTo(1);
    }
}
