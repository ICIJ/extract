package org.icij.extract.document;

import org.apache.tika.Tika;
import org.apache.tika.metadata.Metadata;
import org.junit.Test;

import java.lang.module.ModuleDescriptor;
import java.nio.charset.Charset;
import java.nio.file.Path;

import static org.fest.assertions.Assertions.assertThat;
import static org.icij.extract.document.TikaDocument.TIKA_VERSION;

public class TikaDocumentTest {
    @Test
    public void test_version() {
        TikaDocument tikaDocument = new DocumentFactory().withIdentifier(
                new DigestIdentifier("SHA-256", Charset.defaultCharset())).create("/path/to/doc");
        assertThat(tikaDocument.getTikaVersion().equals(
                ModuleDescriptor.Version.parse(Tika.getString().replace("Apache Tika", "").strip())
        ));
    }

    @Test
    public void test_version_in_metadata() {
        Metadata metadata = new Metadata();
        metadata.set(TIKA_VERSION, "Apache Tika 1.0.0");
        TikaDocument document = new DocumentFactory().withIdentifier(
                new DigestIdentifier("SHA-256", Charset.defaultCharset())).create(Path.of("/path/to/doc"), metadata);
        assertThat(document.getTikaVersion().compareTo(ModuleDescriptor.Version.parse("1.0.0"))).isEqualTo(0);
    }
}