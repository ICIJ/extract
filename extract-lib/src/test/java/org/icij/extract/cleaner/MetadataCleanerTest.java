package org.icij.extract.cleaner;

import org.icij.extract.document.TikaDocument;
import org.icij.extract.extractor.Extractor;
import org.icij.spewer.Spewer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.Reader;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.fest.assertions.Assertions.assertThat;

public class MetadataCleanerTest {
    @Rule public TemporaryFolder fs = new TemporaryFolder();
    private final Extractor extractor = new Extractor();

    @Test
    public void test_remove_metadata_for_pdf_file() throws Exception {
        DocumentSource extractedDocument = new MetadataCleaner().clean(Paths.get(getClass().getResource("/documents/ocr/embedded.pdf").toURI()));

        Path cleanedPdf = fs.newFile("doc.pdf").toPath();
        Files.write(cleanedPdf, extractedDocument.getContent());

        TikaDocument pdfExtracted = extractor.extract(cleanedPdf);
        try (Reader reader = pdfExtracted.getReader()) {
            Spewer.toString(reader);
        }
        assertThat(pdfExtracted.getMetadata().names()).excludes("meta:save-date", "meta:creation-date");
    }

    @Test
    public void test_remove_metadata_for_word_document_file() throws Exception {
        DocumentSource extractedDocument = new MetadataCleaner().clean(Paths.get(getClass().getResource("/documents/office_document.doc").toURI()));

        Path cleanedDoc = fs.newFile("doc.doc").toPath();
        Files.write(cleanedDoc, extractedDocument.getContent());

        TikaDocument docExtracted = extractor.extract(cleanedDoc);
        try (Reader reader = docExtracted.getReader()) {
            Spewer.toString(reader);
        }
        assertThat(docExtracted.getMetadata().names()).excludes("meta:save-date", "meta:creation-date", "Author", "Creation-Date");
    }

    @Test
    public void test_do_not_alter_if_document_type_is_not_supported() throws Exception {
        DocumentSource extractedDocument = new MetadataCleaner().clean(Paths.get(getClass().getResource("/documents/embedded_file_bug.eml").toURI()));

        assertThat(extractedDocument.getContent().length).isEqualTo(16187);
    }

    @Test
    public void test_remove_metadata_for_inputstream() throws Exception {
        DocumentSource extractedDocument = new MetadataCleaner().clean(getClass().getResource("/documents/ocr/embedded.pdf").openStream());

        Path cleanedPdf = fs.newFile("doc.pdf").toPath();
        Files.write(cleanedPdf, extractedDocument.getContent());

        TikaDocument pdfExtracted = extractor.extract(cleanedPdf);
        try (Reader reader = pdfExtracted.getReader()) {
            Spewer.toString(reader);
        }
        assertThat(pdfExtracted.getMetadata().names()).excludes("meta:save-date", "meta:creation-date");
    }
}
