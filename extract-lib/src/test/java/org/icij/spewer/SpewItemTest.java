package org.icij.spewer;

import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.PathIdentifier;
import org.icij.extract.document.TikaDocument;
import org.junit.Test;

import java.nio.file.Paths;

import static org.fest.assertions.Assertions.assertThat;

public class SpewItemTest {

    @Test
    public void testSpewItemExposesItsFields() {
        TikaDocument root = new DocumentFactory().withIdentifier(new PathIdentifier()).create(Paths.get("root"));
        TikaDocument embed = new DocumentFactory().withIdentifier(new PathIdentifier()).create(Paths.get("embed"));

        SpewItem item = new SpewItem(embed, root, root, 1);

        assertThat(item.embed()).isSameAs(embed);
        assertThat(item.parent()).isSameAs(root);
        assertThat(item.root()).isSameAs(root);
        assertThat(item.level()).isEqualTo(1);
    }
}
