package org.icij.extract.extractor;

import org.apache.tika.parser.utils.CommonsDigester;
import org.icij.extract.document.DigestIdentifier;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.TikaDocument;
import org.icij.extract.document.TikaDocumentSource;
import org.icij.extract.extractor.EmbeddedDocumentMemoryExtractor.ContentNotFoundException;
import org.icij.spewer.Spewer;
import org.junit.Before;
import org.junit.Test;

import java.io.Reader;
import java.nio.charset.Charset;

import static org.fest.assertions.Assertions.assertThat;
import static org.fest.assertions.Fail.fail;

public class EmbeddedDocumentMemoryExtractorTest {
    private TikaDocument tikaDocument;

    @Before
    public void setUp() throws Exception {
        tikaDocument = new DocumentFactory().withIdentifier(new DigestIdentifier("SHA-256", Charset.defaultCharset())).
                create(getClass().getResource("/documents/recursive_embedded.docx"));
    }

    @Test
    public void test_embedded_file_not_found() throws Exception {
        try {
            new EmbeddedDocumentMemoryExtractor(new UpdatableDigester("prj", "SHA-256")).
                    extract(tikaDocument, "unknownDigest");
            fail("NullPointerException should have been thrown");
        } catch (ContentNotFoundException npe) {
            assertThat(npe.getMessage()).contains("<unknownDigest> embedded document not found in root document");
            assertThat(npe.getMessage()).contains("documents/recursive_embedded.docx");
        }
    }

    @Test
    public void test_embedded_file_extraction_level_1() throws Exception {
        TikaDocumentSource emfImage = new EmbeddedDocumentMemoryExtractor(new UpdatableDigester("prj", "SHA-256")).
                extract(tikaDocument, "1eeb334ca60c61baca50b9df851b60c52b856c727932d0d1cae4e56a34190e7e");

        assertThat(emfImage).isNotNull();
        assertThat(emfImage.content).hasSize(4992);
    }

    @Test
    public void test_embedded_file_extraction_level_2() throws Exception {
        TikaDocumentSource textContent = new EmbeddedDocumentMemoryExtractor(new UpdatableDigester("prj", "SHA-256")).
                extract(tikaDocument, "13d7b88767d478c03a3f9b01649297f254b3f0845ca1728658d7f7b922d28a32");

        assertThat(textContent).isNotNull();
        assertThat(new String(textContent.content)).isEqualTo("embed_1a");
    }

    @Test
    public void test_embedded_file_extraction_level_4() throws Exception {
        TikaDocumentSource textContent = new EmbeddedDocumentMemoryExtractor(new UpdatableDigester("prj", "SHA-256")).
                extract(tikaDocument, "c6eb226439d6a1a38ad23d69a98b2f321024eeb9ba32cd0238cc39a3d44b5130");

        assertThat(textContent).isNotNull();
        assertThat(new String(textContent.content)).isEqualTo("embed_4");
    }

    @Test
    public void test_embedded_file_extraction_level_2_sha384() throws Exception {
        TikaDocument tikaDocument384 = new DocumentFactory().withIdentifier(new DigestIdentifier("SHA-384", Charset.defaultCharset())).
                        create(getClass().getResource("/documents/recursive_embedded.docx"));
        TikaDocumentSource textContent = new EmbeddedDocumentMemoryExtractor(new UpdatableDigester("prj", "SHA-384")).
                extract(tikaDocument384, "ad1892526e4c1c1c967390da3a8354b6926b03680156d7d76274e6971248be965bc15998a0260f19b801012227f03fae");

        assertThat(textContent).isNotNull();
        assertThat(new String(textContent.content)).isEqualTo("embed_2b");
    }

    @Test
    public void test_embedded_file_extraction_bug() throws Exception {
        TikaDocument tikaDocument384 = new DocumentFactory().withIdentifier(new DigestIdentifier("SHA-256", Charset.defaultCharset())).
                        create(getClass().getResource("/documents/embedded_file_bug.eml"));
        TikaDocumentSource textContent = new EmbeddedDocumentMemoryExtractor(new CommonsDigester(1024*1024*20, "SHA256"), "SHA-256").
                extract(tikaDocument384, "378c7eb3bc002966bf0d9f650efa5811c24787e93418a622bca69842d98bf518");

        assertThat(textContent).isNotNull();
        assertThat(new String(textContent.content)).hasSize(10094);
    }

    @Test
    public void test_embedded_file_content_extraction_should_have_same_hashes_than_extracted_docs() throws Exception {
        Extractor extractor = new Extractor();
        extractor.setDigester(new UpdatableDigester("prj", "SHA-256"));
        try (Reader reader = extractor.extract(tikaDocument)) {
            Spewer.toString(reader);
        }

        EmbeddedDocumentMemoryExtractor contentExtractor = new EmbeddedDocumentMemoryExtractor(
                new UpdatableDigester("prj", "SHA-256"));

        assertThat(tikaDocument.getEmbeds()).hasSize(2);
        assertThat(contentExtractor.extract(tikaDocument, tikaDocument.getEmbeds().get(0).getId())).isNotNull();
        assertThat(contentExtractor.extract(tikaDocument, tikaDocument.getEmbeds().get(1).getId())).isNotNull();
        assertThat(contentExtractor.extract(tikaDocument, tikaDocument.getEmbeds().get(1).getEmbeds().get(0).getId())).isNotNull();
    }
}
