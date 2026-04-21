package org.icij.extract.document;


import org.apache.tika.Tika;
import org.junit.Test;

import java.lang.module.ModuleDescriptor;
import java.nio.charset.Charset;

import static org.fest.assertions.Assertions.assertThat;

public class TikaDocumentTest {
    @Test
    public void test_version() {
        TikaDocument tikaDocument = new DocumentFactory().withIdentifier(
                new DigestIdentifier("SHA-256", Charset.defaultCharset())).create("/path/to/doc");
        assertThat(tikaDocument.getTikaVersion().equals(
                ModuleDescriptor.Version.parse(Tika.getString().replace("Apache Tika", "").strip())
        ));
    }
}