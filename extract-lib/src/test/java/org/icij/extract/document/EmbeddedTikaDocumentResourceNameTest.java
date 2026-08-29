package org.icij.extract.document;

import org.apache.tika.metadata.Metadata;
import org.junit.Test;

import java.nio.file.Paths;

import static org.apache.tika.metadata.TikaCoreProperties.RESOURCE_NAME_KEY;
import static org.fest.assertions.Assertions.assertThat;

public class EmbeddedTikaDocumentResourceNameTest {

    private TikaDocument root() {
        return new DocumentFactory().withIdentifier(new PathIdentifier()).create(Paths.get("/tmp/container.zip"));
    }

    @Test
    public void testNamelessEmbedDoesNotInheritTheRootFileName() {
        Metadata metadata = new Metadata();
        root().addEmbed(metadata);
        assertThat(metadata.get(RESOURCE_NAME_KEY)).isNull();
    }

    @Test
    public void testEmbedKeepsTheNameItsContainerParserSupplied() {
        Metadata metadata = new Metadata();
        metadata.set(RESOURCE_NAME_KEY, "entry.pdf");
        root().addEmbed(metadata);
        assertThat(metadata.get(RESOURCE_NAME_KEY)).isEqualTo("entry.pdf");
    }
}
