package org.icij.extract.extractor;

import org.icij.extract.document.DigestIdentifier;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.TikaDocument;
import org.icij.extract.document.TikaDocumentSource;
import org.icij.spewer.Spewer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.fest.assertions.Assertions.assertThat;

public class MetadataCleanerTest {
    @Rule public TemporaryFolder fs = new TemporaryFolder();
    DocumentFactory documentFactory = new DocumentFactory().withIdentifier(new DigestIdentifier("SHA-256", Charset.defaultCharset()));
    private Extractor extractor = new Extractor();

    @Test
    public void test_remove_metadata_for_pdf_file() throws Exception {
        TikaDocument tikaDocument = documentFactory.create(getClass().getResource("/documents/ocr/embedded.pdf"));
        TikaDocumentSource extractedDocument = new MetadataCleaner().extract(tikaDocument);

        Path cleanedPdf = fs.newFile("doc.pdf").toPath();
        Files.write(cleanedPdf, extractedDocument.content);

        TikaDocument pdfExtracted = extractor.extract(cleanedPdf);
        try (Reader reader = pdfExtracted.getReader()) {
            Spewer.toString(reader);
        }
        assertThat(pdfExtracted.getMetadata().names()).excludes("meta:save-date", "meta:creation-date");
    }
}
