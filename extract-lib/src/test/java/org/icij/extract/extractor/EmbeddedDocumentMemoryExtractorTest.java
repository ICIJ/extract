package org.icij.extract.extractor;

import org.junit.Test;

import java.io.InputStream;

import static org.fest.assertions.Assertions.assertThat;

public class EmbeddedDocumentMemoryExtractorTest {
    @Test
    public void test_embedded_file_extraction_level_1() throws Exception {
        InputStream stream = getClass().getResourceAsStream("/documents/recursive_embedded.docx");

        byte[] emfImage = new EmbeddedDocumentMemoryExtractor("prj", "SHA-256").
                extract(stream, "c15ee2aa9c703e47d86993345ed5567c46441a39fa29a26a860af490c251564e");

        assertThat(emfImage).isNotNull();
        assertThat(emfImage).hasSize(4992);
    }

    @Test
    public void test_embedded_file_extraction_level_2() throws Exception {
        InputStream stream = getClass().getResourceAsStream("/documents/recursive_embedded.docx");

        byte[] textContent = new EmbeddedDocumentMemoryExtractor("prj", "SHA-256").
                extract(stream, "b67d5e9b7876b9345c2a7d611ec4e7a2c5c143ab97267b42ef698f062abcabb4");

        assertThat(textContent).isNotNull();
        assertThat(new String(textContent)).isEqualTo("embed_1a");
    }

    @Test
    public void test_embedded_file_extraction_level_4() throws Exception {
        InputStream stream = getClass().getResourceAsStream("/documents/recursive_embedded.docx");

        byte[] textContent = new EmbeddedDocumentMemoryExtractor("prj", "SHA-256").
                extract(stream, "7351c5258595692cc9813926218be0cd1bcaa6bc374d89a74b6266bb9a11d398");

        assertThat(textContent).isNotNull();
        assertThat(new String(textContent)).isEqualTo("embed_4");
    }

    @Test
    public void test_embedded_file_extraction_level_2_sha384() throws Exception {
        InputStream stream = getClass().getResourceAsStream("/documents/recursive_embedded.docx");

        byte[] textContent = new EmbeddedDocumentMemoryExtractor("prj", "SHA-384").
                extract(stream, "50a4893b3f4b9390061c28d09db0650ef9b30b3d32c3cc7b56349df5a6db0dc6707fd4bb7a9b5c6b0b38c1ce6b990c08");

        assertThat(textContent).isNotNull();
        assertThat(new String(textContent)).isEqualTo("embed_2b");
    }
}
