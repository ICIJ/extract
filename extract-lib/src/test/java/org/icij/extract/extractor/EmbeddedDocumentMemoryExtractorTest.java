package org.icij.extract.extractor;

import org.apache.tika.parser.digestutils.CommonsDigester;
import org.icij.extract.document.DigestIdentifier;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.TikaDocument;
import org.icij.extract.document.TikaDocumentSource;
import org.icij.extract.extractor.EmbeddedDocumentExtractor.ContentNotFoundException;
import org.icij.spewer.Spewer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.file.Paths;

import static org.fest.assertions.Assertions.assertThat;
import static org.fest.assertions.Fail.fail;

public class EmbeddedDocumentMemoryExtractorTest {

    private TikaDocument tikaDocument;
    DocumentFactory documentFactory = new DocumentFactory().withIdentifier(new DigestIdentifier("SHA-256", Charset.defaultCharset()));
    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    @Before
    public void setUp() throws Exception {
        tikaDocument = documentFactory.create(getClass().getResource("/documents/recursive_embedded.docx"));
    }

    @After
    public void tearDown() throws Exception {
        TikaVersionTestHelper.restoreVersion();
    }

    @Test
    public void test_embedded_file_not_found() throws Exception {
        //GIVEN
        EmbeddedDocumentExtractor extractor = new EmbeddedDocumentExtractor(
                new UpdatableDigester("prj", "SHA-256"), tmp.getRoot().toPath());

        try {
            //WHEN
            extractor.extract(tikaDocument, "unknownDigest").getContent();
            fail("NullPointerException should have been thrown");
        } catch (ContentNotFoundException npe) {
            //THEN
            assertThat(npe.getMessage()).contains("<unknownDigest> embedded document not found in root document");
            assertThat(npe.getMessage()).contains("documents/recursive_embedded.docx");
        }
    }

    @Test
    public void test_embedded_file_extraction_level_1() throws Exception {
        //GIVEN
        EmbeddedDocumentExtractor extractor = new EmbeddedDocumentExtractor(
                new UpdatableDigester("prj", "SHA-256"), tmp.getRoot().toPath());

        //WHEN
        TikaDocumentSource emfImage = extractor.extract(tikaDocument,
                "1eeb334ca60c61baca50b9df851b60c52b856c727932d0d1cae4e56a34190e7e");

        //THEN
        assertThat(emfImage).isNotNull();
        assertThat(emfImage.getContent()).hasSize(4992);
        assertThat(EmbeddedDocumentExtractor.getEmbeddedPath(tmp.getRoot().toPath(),
                "1eeb334ca60c61baca50b9df851b60c52b856c727932d0d1cae4e56a34190e7e").toFile()).isFile();
    }

    @Test
    public void test_embedded_memory_extraction_level_1() throws Exception {
        //GIVEN
        EmbeddedDocumentExtractor extractor = new EmbeddedDocumentExtractor(
                new UpdatableDigester("prj", "SHA-256"));

        //WHEN
        TikaDocumentSource emfImage = extractor.extract(tikaDocument,
                "1eeb334ca60c61baca50b9df851b60c52b856c727932d0d1cae4e56a34190e7e");

        //THEN
        assertThat(emfImage).isNotNull();
        assertThat(emfImage.getContent()).hasSize(4992);
    }

    @Test
    public void test_embedded_file_extraction_level_1_should_use_cache_if_it_exists() throws Exception {
        //GIVEN
        EmbeddedDocumentExtractor extractor = new EmbeddedDocumentExtractor(
                new UpdatableDigester("prj", "SHA-256"), tmp.getRoot().toPath());
        extractor.extract(tikaDocument, "1eeb334ca60c61baca50b9df851b60c52b856c727932d0d1cae4e56a34190e7e");

        //WHEN
        TikaDocumentSource emfImage = extractor.extract(null, "1eeb334ca60c61baca50b9df851b60c52b856c727932d0d1cae4e56a34190e7e");

        //THEN
        assertThat(emfImage).isNotNull();
        assertThat(emfImage.metadata()).isNotNull();
        assertThat(emfImage.getContent()).hasSize(4992);
    }

    @Test
    public void test_extract_all_embedded_artifacts_from_root_document() throws Exception {
        //GIVEN
        EmbeddedDocumentExtractor extractor = new EmbeddedDocumentExtractor(
                new UpdatableDigester("prj", "SHA-256"), tmp.getRoot().toPath().resolve("prj"));

        //WHEN
        extractor.extractAll(tikaDocument);

        //THEN
        assertThat(tmp.getRoot().toPath().resolve("prj").toFile()).isDirectory();
        assertThat(tmp.getRoot().toPath().resolve("prj").toFile().listFiles()).hasSize(11);
    }

    @Test(expected = IllegalStateException.class)
    public void test_extract_all_embedded_artifacts_from_root_document_without_path() throws Exception {
        //GIVEN
        EmbeddedDocumentExtractor extractor = new EmbeddedDocumentExtractor(
                new UpdatableDigester("prj", "SHA-256"));
        
        //WHEN/THEN
        extractor.extractAll(null);
    }

    @Test
    public void test_embedded_file_extraction_level_2() throws Exception {
        //GIVEN
        EmbeddedDocumentExtractor extractor = new EmbeddedDocumentExtractor(
                new UpdatableDigester("prj", "SHA-256"), tmp.getRoot().toPath());

        //WHEN
        TikaDocumentSource textContent = extractor.extract(tikaDocument,
                "13d7b88767d478c03a3f9b01649297f254b3f0845ca1728658d7f7b922d28a32");

        //THEN
        assertThat(textContent).isNotNull();
        assertThat(new String(textContent.getContent())).isEqualTo("embed_1a");
    }

    @Test
    public void test_embedded_file_extraction_level_4() throws Exception {
        //GIVEN
        EmbeddedDocumentExtractor extractor = new EmbeddedDocumentExtractor(
                new UpdatableDigester("prj", "SHA-256"), tmp.getRoot().toPath());

        //WHEN
        TikaDocumentSource textContent = extractor.extract(tikaDocument,
                "c6eb226439d6a1a38ad23d69a98b2f321024eeb9ba32cd0238cc39a3d44b5130");

        //THEN
        assertThat(textContent).isNotNull();
        assertThat(new String(textContent.getContent())).isEqualTo("embed_4");
    }

    @Test
    public void test_embedded_file_extraction_level_2_sha384() throws Exception {
        //GIVEN
        TikaDocument tikaDocument384 = new DocumentFactory()
                .withIdentifier(new DigestIdentifier("SHA-384", Charset.defaultCharset()))
                .create(getClass().getResource("/documents/recursive_embedded.docx"));
        EmbeddedDocumentExtractor extractor = new EmbeddedDocumentExtractor(
                new UpdatableDigester("prj", "SHA-384"), tmp.getRoot().toPath());

        //WHEN
        TikaDocumentSource textContent = extractor.extract(tikaDocument384,
                "ad1892526e4c1c1c967390da3a8354b6926b03680156d7d76274e6971248be965bc15998a0260f19b801012227f03fae");

        //THEN
        assertThat(textContent).isNotNull();
        assertThat(new String(textContent.getContent())).isEqualTo("embed_2b");
    }

    @Test
    public void test_embedded_file_extraction_bug() throws Exception {
        //GIVEN
        TikaDocument tikaDocument256 = new DocumentFactory()
                .withIdentifier(new DigestIdentifier("SHA-256", Charset.defaultCharset()))
                .create(getClass().getResource("/documents/embedded_file_bug.eml"));
        EmbeddedDocumentExtractor extractor = new EmbeddedDocumentExtractor(
                new CommonsDigester(1024 * 1024 * 20, "SHA256"), "SHA-256", tmp.getRoot().toPath(), false);

        //WHEN
        TikaDocumentSource pngFile = extractor.extract(tikaDocument256,
                "dae37ba1313e9724b29eab8f3c8b4ec267482023866c5528b95c1d306786c32a");

        //THEN
        assertThat(pngFile).isNotNull();
        assertThat(new String(pngFile.getContent())).hasSize(634);
    }

    @Test
    public void test_embedded_file_content_extraction_should_have_same_hashes_than_extracted_docs() throws Exception {
        //GIVEN
        Extractor extractor = new Extractor(documentFactory);
        extractor.setDigester(new UpdatableDigester("prj", "SHA-256"));
        TikaDocument extracted = extractor.extract(Paths.get(getClass().getResource("/documents/recursive_embedded.docx").getPath()));
        try (Reader reader = extracted.getReader()) {
            Spewer.toString(reader);
        }
        EmbeddedDocumentExtractor contentExtractor = new EmbeddedDocumentExtractor(
                new UpdatableDigester("prj", "SHA-256"), tmp.getRoot().toPath());

        //WHEN/THEN
        assertThat(extracted.getEmbeds()).hasSize(1);
        assertThat(contentExtractor.extract(extracted, extracted.getEmbeds().get(0).getId())).isNotNull();
        assertThat(contentExtractor.extract(extracted, extracted.getEmbeds().get(0).getId())).isNotNull();
        assertThat(contentExtractor.extract(extracted, extracted.getEmbeds().get(0).getEmbeds().get(0).getId())).isNotNull();
    }

    @Test
    public void test_embedded_bug_732() throws Exception {
        //GIVEN
        Extractor extractor = new Extractor(documentFactory);
        extractor.setDigester(new UpdatableDigester("prj", "SHA-256"));
        TikaDocument extracted = extractor.extract(Paths.get(getClass().getResource("/documents/3rd-level-bug-732.msg").getPath()));
        try (Reader reader = extracted.getReader()) {
            Spewer.toString(reader);
        }
        EmbeddedDocumentExtractor contentExtractor = new EmbeddedDocumentExtractor(
                new UpdatableDigester("prj", "SHA-256"), tmp.getRoot().toPath());

        //WHEN/THEN
        assertThat(extracted.getEmbeds()).hasSize(1);
        assertThat(contentExtractor.extract(extracted, extracted.getEmbeds().get(0).getId())).isNotNull();
        assertThat(contentExtractor.extract(extracted, extracted.getEmbeds().get(0).getEmbeds().get(0).getId())).isNotNull();
        assertThat(contentExtractor.extract(extracted, extracted.getEmbeds().get(0).getEmbeds().get(0).getEmbeds().get(0).getId())).isNotNull();
        assertThat(contentExtractor.extract(extracted, extracted.getEmbeds().get(0).getEmbeds().get(0).getEmbeds().get(0).getId()).
                metadata().get("resourceName")).contains("POD Layout ICIJ 2020.pdf");
    }

    @Test
    public void test_embedded_bug_732_tika330_retro_compatibility() throws Exception {
        //GIVEN
        TikaVersionTestHelper.setVersion("3.2.3");
        TikaDocument tikaDocument256 = new DocumentFactory()
                .withIdentifier(new DigestIdentifier("SHA-256", Charset.defaultCharset()))
                .create(getClass().getResource("/documents/3rd-level-bug-732.msg"));
        EmbeddedDocumentExtractor contentExtractor = new EmbeddedDocumentExtractor(
                new UpdatableDigester("prj", "SHA-256"), "SHA-256", tmp.getRoot().toPath(), false);

        //WHEN
        TikaDocumentSource result = contentExtractor.extract(tikaDocument256,
                "72d03fc3c34e06df808ab357629d039121568f623c0c7814bc207841f1b54a44");

        //THEN
        assertThat(result).isNotNull();
    }

    @Test
    public void test_extract_embedded_without_ocr() throws Exception {
        //GIVEN
        EmbeddedDocumentExtractor contentExtractor = new EmbeddedDocumentExtractor(
                new CommonsDigester(20 * 1024 * 1024, CommonsDigester.DigestAlgorithm.SHA256.toString()),
                "SHA-256", tmp.getRoot().toPath(), false);

        //WHEN
        TikaDocumentSource actual = contentExtractor.extract(
                documentFactory.create(Paths.get(getClass().getResource("/documents/embedded_with_duplicate.tgz").getPath())),
                "2519f5fc76b8e243c8b0ae42cbee55afd3b0c0ffe67d31a5a8f2a9b13f2998e8");

        //THEN
        assertThat(new String(actual.getContent()).replace("\n", "")).isEqualTo("level2");
    }

    @Test
    public void test_hash_with_ocr_and_without_ocr_is_the_same() throws Exception {
        //GIVEN
        EmbeddedDocumentExtractor ocrExtractor = new EmbeddedDocumentExtractor(
                new CommonsDigester(20 * 1024 * 1024, CommonsDigester.DigestAlgorithm.SHA256.toString()),
                "SHA-256", tmp.getRoot().toPath(), true);
        EmbeddedDocumentExtractor noOcrExtractor = new EmbeddedDocumentExtractor(
                new CommonsDigester(20 * 1024 * 1024, CommonsDigester.DigestAlgorithm.SHA256.toString()),
                "SHA-256", tmp.getRoot().toPath(), false);
        
        //WHEN
        TikaDocument doc = documentFactory.create(Paths.get(getClass().getResource("/documents/embedded_with_duplicate.tgz").getPath()));
        String digest = "d4f96c1c29d838a99e95b72bfd949f2cf802afddefa1e1d92e358e15bac5abcd";

        //THEN
        assertThat(ocrExtractor.extract(doc, digest)).isNotNull();
        assertThat(noOcrExtractor.extract(doc, digest)).isNotNull();
    }

    @Test
    public void test_embedded_path() {
        //GIVEN/WHEN
        String path = EmbeddedDocumentExtractor.getEmbeddedPath(Paths.get("/tmp"), "1234digest").toString();

        //THEN
        assertThat(path).isEqualTo("/tmp/12/34/1234digest/raw");
    }

    final String onePixelJpg =
            "ffd8 ffdb 0043 0001 0101 0101 0101 0101 0101 0101 0101 0101 0101 0101 0101 0101 0101 " +
                    "0101 0101 0101 0101 0101 0101 0101 0101 0101 0101 0101 0101 0101 0101 0101 0101 0101 " +
                    "0101 01ff c200 0b08 0001 0001 0101 1100 ffc4 0014 0001 0000 0000 0000 0000 0000 0000 " +
                    "0000 0003 ffda 0008 0101 00000070 0000 0001 3fff d9";


    public static byte[] hexToBin(String inputHexadecimal) {
        String str = inputHexadecimal.replaceAll(" ", "");
        int len = str.length();
        byte[] out = new byte[len / 2];
        int endIndx;

        for (int i = 0; i < len; i = i + 2) {
            endIndx = i + 2;
            if (endIndx > len)
                endIndx = len - 1;
            out[i / 2] = (byte) Integer.parseInt(str.substring(i, endIndx), 16);
        }
        return out;
    }
}
