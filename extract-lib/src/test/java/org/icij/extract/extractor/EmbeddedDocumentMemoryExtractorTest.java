package org.icij.extract.extractor;

import org.junit.Test;

import java.io.InputStream;

import static org.fest.assertions.Assertions.assertThat;

public class EmbeddedDocumentMemoryExtractorTest {
    @Test
    public void test_embedded_file_extraction_level_1() throws Exception {
        InputStream stream = getClass().getResourceAsStream("/documents/recursive_embedded.docx");

        byte[] emfImage = new EmbeddedDocumentMemoryExtractor("prj", "SHA-256").extract(stream, "c15ee2aa9c703e47d86993345ed5567c46441a39fa29a26a860af490c251564e");

        assertThat(emfImage).isNotNull();
        assertThat(emfImage).hasSize(4992);
    }
}
