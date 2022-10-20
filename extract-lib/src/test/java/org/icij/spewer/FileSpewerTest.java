package org.icij.spewer;

import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.PathIdentifier;
import org.icij.extract.document.TikaDocument;
import org.icij.extract.extractor.Extractor;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.StringReader;
import java.nio.charset.Charset;
import java.nio.file.Path;

import static java.nio.file.Files.readAllLines;
import static java.nio.file.Paths.get;
import static org.fest.assertions.Assertions.assertThat;

public class FileSpewerTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    private final DocumentFactory factory = new DocumentFactory().withIdentifier(new PathIdentifier());
    private FileSpewer fileSpewer = new FileSpewer(new FieldNames());

    @Test
    public void test_spew_root_document_in_memory() throws Exception {
        final TikaDocument tikaDocument = factory.create("test.txt");
        tikaDocument.setReader(new StringReader("this is a content"));

        fileSpewer.writeDocument(tikaDocument, null, null, 0);

        String[] list = folder.getRoot().list();
        assertThat(list).isNotEmpty();
        assertThat(list).contains("test.txt.json", "test.txt.txt");
        assertThat( readAllLines(folder.getRoot().toPath().resolve("test.txt.json"), Charset.defaultCharset())).contains("{ }");
        assertThat( readAllLines(folder.getRoot().toPath().resolve("test.txt.txt"), Charset.defaultCharset())).contains("this is a content");
    }

    @Test
    public void test_spew_root_document_on_filesystem() throws Exception {
        final Extractor extractor = new Extractor(factory);
        Path path = get(getClass().getResource("/documents/text/plain.txt").getPath());
        TikaDocument tikaDocument = extractor.extract(path);

        fileSpewer.writeDocument(tikaDocument, null, null, 0);

        String[] list = folder.getRoot().list();
        assertThat(list).isNotEmpty();
        assertThat(list).contains("home");
        assertThat(readAllLines(get(folder.getRoot().toPath().resolve(path.toString().substring(1)).toString() + ".json"), Charset.defaultCharset())).
                hasSize(9);
        assertThat(readAllLines(get(folder.getRoot().toPath().resolve(path.toString().substring(1)).toString() + ".txt"), Charset.defaultCharset())).
                contains("This is a test.");
    }

    @Before
    public void setUp() {
        fileSpewer.setOutputDirectory(folder.getRoot().toPath());
    }
}
