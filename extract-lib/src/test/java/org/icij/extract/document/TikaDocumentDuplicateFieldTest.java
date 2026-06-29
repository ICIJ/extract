package org.icij.extract.document;

import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import static org.fest.assertions.Assertions.assertThat;

public class TikaDocumentDuplicateFieldTest {

    // The streaming spew worker reads isDuplicate on a different thread from the one that sets it
    // (the foreground writeDocument(root) call). The field must be volatile for visibility.
    @Test
    public void testIsDuplicateFieldIsVolatile() throws Exception {
        final Field field = TikaDocument.class.getDeclaredField("isDuplicate");
        assertThat(Modifier.isVolatile(field.getModifiers()))
                .as("TikaDocument.isDuplicate must be volatile for cross-thread duplicate-gate visibility")
                .isTrue();
    }
}
