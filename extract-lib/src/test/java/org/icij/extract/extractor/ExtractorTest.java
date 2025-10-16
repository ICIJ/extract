package org.icij.extract.extractor;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.tika.Tika;
import org.apache.tika.exception.EncryptedDocumentException;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.EmbeddedTikaDocument;
import org.icij.extract.document.TikaDocument;
import org.icij.extract.ocr.Tess4JOCRConfigAdapter;
import org.icij.extract.ocr.Tess4JOCRParser;
import org.icij.spewer.FieldNames;
import org.icij.spewer.FileSpewer;
import org.icij.spewer.PrintStreamSpewer;
import org.icij.spewer.Spewer;
import org.icij.task.Options;
import org.icij.test.CauseMatcher;
import org.icij.test.RegexMatcher;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static java.lang.Math.toIntExact;
import static org.fest.assertions.Assertions.assertThat;
import static org.icij.extract.ocr.OCRParser.OCR_PARSER;
import static org.icij.extract.ocr.ParserWithConfidence.OCR_CONFIDENCE;

public class ExtractorTest {
	@Rule public final ExpectedException thrown = ExpectedException.none();
	@Rule public final TemporaryFolder folder = new TemporaryFolder();

	@Test
	public void testTikaVersion() throws Throwable {
        Extractor extractor = new Extractor();
		TikaDocument tikaDocument = extractor.extract(Paths.get(getClass().getResource("/documents/ocr/simple.tiff").getPath()));
		Assert.assertEquals(Tika.getString(), tikaDocument.getMetadata().get(TikaDocument.TIKA_VERSION));
	}

	@Test
	public void testOcr() throws Throwable {
		String text;
        Extractor extractor = new Extractor();
		TikaDocument tikaDocument = extractor.extract(Paths.get(getClass().getResource("/documents/ocr/simple.tiff").getPath()));
		try (Reader reader = tikaDocument.getReader()) {
			text = Spewer.toString(reader);
		}
        assertThat(tikaDocument.getMetadata().get(OCR_PARSER)).isNotNull();
        assertThat(tikaDocument.getMetadata().get(OCR_PARSER)).isEqualTo("org.icij.extract.ocr.TesseractOCRParser");

		Assert.assertEquals("image/tiff", tikaDocument.getMetadata().get(Metadata.CONTENT_TYPE));
		Assert.assertEquals("HEAVY\nMETAL", text.trim());
	}

	@Test
	public void testExtractorShouldSupportMultipleLanguage() throws Throwable {
		// When
        Extractor extractor = new Extractor();
		extractor.setOcrLanguage("eng+spa");
		String text;
		TikaDocument tikaDocument = extractor.extract(Paths.get(getClass().getResource("/documents/ocr/simple.tiff").getPath()));
		try (Reader reader = tikaDocument.getReader()) {
			text = Spewer.toString(reader);
		}

		Assert.assertEquals("image/tiff", tikaDocument.getMetadata().get(Metadata.CONTENT_TYPE));
		Assert.assertEquals("HEAVY\nMETAL", text.trim());
		// We expect a null language, downstream component will be responsible to guess the language from the content
		assertThat(tikaDocument.getLanguage()).isNull();
	}

	@Test
	public void testDisableOcr() throws Throwable {
        Extractor extractor = new Extractor();
		extractor.disableOcr();

		TikaDocument tikaDocument = extractor.extract(Paths.get(getClass().getResource("/documents/ocr/simple.tiff").getPath()));

		final int read = tikaDocument.getReader().read();

		Assert.assertEquals("image/tiff", tikaDocument.getMetadata().get(Metadata.CONTENT_TYPE));
		Assert.assertEquals(-1, read);
        assertThat(tikaDocument.getMetadata().get(OCR_PARSER)).isNull();
    }

	@Test
	public void testCSVFile() throws Throwable {
        Extractor extractor = new Extractor();
		String text;

		TikaDocument tikaDocument = extractor.extract(Paths.get(getClass().getResource("/documents/csv_document.csv").getPath()));

		try (Reader reader = tikaDocument.getReader()) {
			text = Spewer.toString(reader);
		}

		Assert.assertEquals("text/csv; charset=UTF-8; delimiter=comma", tikaDocument.getMetadata().get(Metadata.CONTENT_TYPE));
		Assert.assertEquals(12, text.lines().count());
	}

	@Test
	public void testRtfFile() throws Throwable {
        Extractor extractor = new Extractor();
		String text;
		TikaDocument tikaDocument = extractor.extract(Paths.get(getClass().getResource("/documents/text/doc.rtf").getPath()));
		try (Reader reader = tikaDocument.getReader()) {
			text = Spewer.toString(reader);
		}

		Assert.assertEquals("application/rtf", tikaDocument.getMetadata().get(Metadata.CONTENT_TYPE));
		Assert.assertEquals("RTF Text Document", text.trim());
	}

	@Test
	public void testFileNotFound() throws Throwable {
        Extractor extractor = new Extractor();
		thrown.expect(NoSuchFileException.class);
		thrown.expectMessage("nothing");

		extractor.extract(Paths.get("nothing"));
	}

	@Test
	public void testEncryptedPdf() throws Throwable {
        Extractor extractor = new Extractor();
		thrown.expect(IOException.class);
		thrown.expectMessage("");
		thrown.expectCause(new CauseMatcher(EncryptedDocumentException.class, "Unable to process: document is encrypted"));


		final int read;
		TikaDocument tikaDocument = extractor.extract(Paths.get(getClass().getResource("/documents/pdf/encrypted.pdf").getPath()));
		try (final Reader reader = tikaDocument.getReader()) {
			read = reader.read();
		} catch (IOException e) {
			Assert.assertEquals("application/pdf", tikaDocument.getMetadata().get(Metadata.CONTENT_TYPE));
			throw e;
		}

		Assert.fail(String.format("Read \"%d\" while expecting exception.", read));
	}

	@Test
	public void testGarbage() throws Throwable {
        Extractor extractor = new Extractor();
		TikaDocument tikaDocument = extractor.extract(Paths.get(getClass().getResource("/documents/garbage.bin").getPath()));


		thrown.expect(IOException.class);
		thrown.expectMessage("");
		thrown.expectCause(new CauseMatcher(TikaException.class, "Parse error"));

		final int read;

		try (final Reader reader = tikaDocument.getReader()) {
			read = reader.read();
		} catch (IOException e) {
			Assert.assertEquals("application/octet-stream", tikaDocument.getMetadata().get(Metadata.CONTENT_TYPE));
			throw e;
		}

		Assert.fail(String.format("Read \"%d\" while expecting exception.", read));
	}

	@Test
	public void testByProjectDigester() throws Exception {
		DocumentFactory documentFactory = new DocumentFactory().configure(Options.from(Map.of("digestAlgorithm", "SHA-384")));
		final Extractor extractor = new Extractor(documentFactory);
		TikaDocument tikaDocument1 = new Extractor(documentFactory, Options.from(Map.of("digestAlgorithm", "SHA-384")))
            .extract(Paths.get(getClass().getResource("/documents/ocr/simple.tiff").getPath()));
		TikaDocument tikaDocument2 = new Extractor(documentFactory, Options.from(Map.of("digestAlgorithm", "SHA-384", "digestProjectName", "project1")))
            .extract(Paths.get(getClass().getResource("/documents/ocr/simple.tiff").getPath()));
		TikaDocument tikaDocument3 = new Extractor(documentFactory, Options.from(Map.of("digestAlgorithm", "SHA-384", "digestProjectName", "project2")))
            .extract(Paths.get(getClass().getResource("/documents/ocr/simple.tiff").getPath()));

		Assert.assertNotEquals(tikaDocument1.getId(), tikaDocument2.getId());
		Assert.assertNotEquals(tikaDocument1.getId(), tikaDocument3.getId());
		Assert.assertNotEquals(tikaDocument2.getId(), tikaDocument3.getId());
	}

	@Test
	public void testDocumentUseCorrectDigestIdentifier () throws Exception {
		Options<String> digestAlgorithm = Options.from(Map.of("digestAlgorithm", "SHA-384", "digestProjectName", "project1"));
		DocumentFactory documentFactory = new DocumentFactory().configure(digestAlgorithm);
		final Extractor extractor = new Extractor(documentFactory, digestAlgorithm);
		TikaDocument tikaDocument1 = extractor.extract(Paths.get(getClass().getResource("/documents/ocr/simple.tiff").getPath()));

		Assert.assertNotNull(tikaDocument1.getId());
	}

	@Test
	public void testDocumentUseCorrectLanguage () throws IOException {
        Options<String> options = Options.from(Map.of("language", "zho"));
		DocumentFactory documentFactory = new DocumentFactory().configure(options);
        final Extractor extractor = new Extractor(documentFactory, options);
		TikaDocument tikaDocument1 = extractor.extract(Paths.get(getClass().getResource("/documents/ocr/simple.tiff").getPath()));

		Assert.assertEquals(tikaDocument1.getLanguage(), "zho");
	}

	@Test
	public void testDocumentHasNoLanguage () throws IOException {
		DocumentFactory documentFactory = new DocumentFactory().configure();
		final Extractor extractor = new Extractor(documentFactory);
		TikaDocument tikaDocument1 = extractor.extract(Paths.get(getClass().getResource("/documents/ocr/simple.tiff").getPath()));

		Assert.assertNull(tikaDocument1.getLanguage());
	}

	@Test
	public void testEmbeds() throws Throwable {
		TikaDocument tikaDocument = new Extractor().extract(Paths.get(getClass().getResource("/documents/ocr/embedded.pdf").getPath()));
		String text;

		try (final Reader reader = tikaDocument.getReader()) {
			text = Spewer.toString(reader);
		}
        assertThat(tikaDocument.getMetadata().get(OCR_PARSER)).isNotNull();
		Assert.assertEquals("application/pdf", tikaDocument.getMetadata().get(Metadata.CONTENT_TYPE));
		Assert.assertThat(text, RegexMatcher.matchesRegex("^\\s+HEAVY\\sMETAL\\s+HEAVY\\sMETAL\\s+$"));
	}

	@Test
	public void testIgnoreEmbeds() throws Throwable {
        Extractor extractor = new Extractor();
		extractor.setEmbedHandling(Extractor.EmbedHandling.IGNORE);
		Assert.assertEquals(extractor.getEmbedHandling(), Extractor.EmbedHandling.IGNORE);

		TikaDocument tikaDocument = extractor.extract(Paths.get(getClass().getResource("/documents/ocr/embedded.pdf").getPath()));

		String text;

		try (final Reader reader = tikaDocument.getReader()) {
			text = Spewer.toString(reader);
		}

        assertThat(tikaDocument.getMetadata().get(OCR_PARSER)).isNull();
		Assert.assertEquals("application/pdf", tikaDocument.getMetadata().get(Metadata.CONTENT_TYPE));
		Assert.assertEquals("\n\n\n\n", text);
	}

	@Test
	public void testDisableOcrOnEmbed() throws Throwable {
        Extractor extractor = new Extractor();
		extractor.disableOcr();

		TikaDocument tikaDocument = extractor.extract(Paths.get(getClass().getResource("/documents/ocr/embedded.pdf").getPath()));

		String text;

		try (final Reader reader = tikaDocument.getReader()) {
			text = Spewer.toString(reader);
		}

        assertThat(tikaDocument.getMetadata().get(OCR_PARSER)).isNull();
		Assert.assertEquals("application/pdf", tikaDocument.getMetadata().get(Metadata.CONTENT_TYPE));
		Assert.assertEquals("\n\n\n\n", text);
	}

	@Test
	public void testHtmlOutput() throws Throwable {
        Extractor extractor = new Extractor();
		extractor.setOutputFormat(Extractor.OutputFormat.HTML);

		TikaDocument tikaDocument = extractor.extract(Paths.get(getClass().getResource("/documents/text/utf16.txt").getPath()));

		String text;
		try (final Reader reader = tikaDocument.getReader()) {
			text = Spewer.toString(reader);
		}

		Assert.assertEquals("text/plain; charset=UTF-16LE", tikaDocument.getMetadata().get(Metadata.CONTENT_TYPE));
		Assert.assertEquals(getExpected("/expected/utf16-txt.html"), text);
	}

	@Test
	public void test_ocr_confidence() throws Exception {
        Extractor extractor = new Extractor(Options.from(Map.of("ocrType", "TESS4J")));

		TikaDocument tikaDocument = extractor.extract(Paths.get(getClass().getResource("/documents/ocr/test.jpeg").getPath()));
        assertThat(tikaDocument.getMetadata().get(OCR_PARSER)).isEqualTo("org.icij.extract.ocr.Tess4JOCRParser");

		assertThat(Optional.ofNullable(tikaDocument.getMetadata().get(OCR_CONFIDENCE)).map(Float::parseFloat).orElse(0.0f))
			.isGreaterThan(0.0f);
	}

	@Test
	public void testHtmlOutputWithEmbeddedEmbeds() throws Throwable {
        Extractor extractor = new Extractor();
		extractor.setOutputFormat(Extractor.OutputFormat.HTML);
		Assert.assertEquals(extractor.getOutputFormat(), Extractor.OutputFormat.HTML);

		TikaDocument tikaDocument = extractor.extract(Paths.get(getClass().getResource("/documents/ocr/embedded.pdf").getPath()));

		String text;
		try (final Reader reader = tikaDocument.getReader()) {
			text = Spewer.toString(reader);
		}

        assertThat(tikaDocument.getMetadata().get(OCR_PARSER)).isNotNull();
		Assert.assertEquals("application/pdf", tikaDocument.getMetadata().get(Metadata.CONTENT_TYPE));
		Assert.assertEquals(getExpected("/expected/embedded-data-uri-pdf.html"), text);
	}

	@Test
	public void testRecursiveEmbedded() throws Exception {
        Extractor extractor = new Extractor();
		TikaDocument tikaDocument = extractor.extract(Paths.get(getClass().getResource("/documents/recursive_embedded.docx").getPath()));

		/* we have to use a spewer and not just extract and check the structure of the tikaDocument
		 because the principle is to stream the embedded documents to avoid mounting all the documents in memory.
		 That could be an issue for big documents like mailboxes, zips or tarballs.

		 see https://cwiki.apache.org/confluence/display/tika/RecursiveMetadata#Jukka.27s_RecursiveMetadata_Parser
		 "A downside to the wrapper is that it breaks the Tika goal of streaming output"

		 so we just use a print stream spewer and check that all the tree has been parsed */
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		new PrintStreamSpewer(new PrintStream(outputStream), new FieldNames()).write(tikaDocument);
		String allContents = outputStream.toString();

		assertThat(allContents).contains("embed_0");
		assertThat(allContents).contains("embed_1a");
		assertThat(allContents).contains("embed_1b");
		assertThat(allContents).contains("embed_2a");
		assertThat(allContents).contains("embed_2b");
		assertThat(allContents).contains("embed_3");
		assertThat(allContents).contains("dissolve the political bands");
		assertThat(allContents).contains("embed_4");
	}

	@Test
	public void testEmbeddedWithDuplicates() throws Exception {
        Extractor extractor = new Extractor();
		extractor.disableOcr();
		extractor.setEmbedOutputPath(folder.newFolder("embeds").toPath());
		/*
		embedded_with_duplicate.tgz :
			2020-09-11 08:56 level1/
			2020-09-08 15:10 level1/one_pixel_level1.jpg
			2020-09-08 15:11 level1/file.txt          |
			2020-09-08 15:11 level1/level2.tgz        |
			2020-09-08 15:10 		level2/          same
			2020-09-08 15:10 		level2/file.txt   |
			2020-09-08 15:10 		level2/one_pixel.jpg
		 */
		TikaDocument tikaDocument = extractor.extract(Paths.get(getClass().getResource("/documents/embedded_with_duplicate.tgz").getPath()));
		FileSpewer fileSpewer = new FileSpewer(new FieldNames());
		fileSpewer.setOutputDirectory(folder.getRoot().toPath());
		fileSpewer.write(tikaDocument);

		/* should find
		 hash(embedded_with_duplicate.tgz)
		 hash(embedded_with_duplicate.tar)
		 hash(level1/file.txt)
		 hash(level1/level2.tgz)
		 hash(level1/level2/file.txt)
		 hash(level1/one_pixel_level1.jpg) = hash(level1/level2/one_pixel.jpg) */
		assertThat(folder.getRoot().toPath().resolve("embeds").toFile().listFiles()).hasSize(6);
	}

	@Test
	public void testPageIndicesExtractionForPdf() throws Exception {
        Extractor extractor = new Extractor();
		TikaDocument doc = extractor.extract(Paths.get(getClass().getResource("/documents/ocr/embedded.pdf").getPath()));
		String text;
		try (final Reader reader = doc.getReader()) {
			text = Spewer.toString(reader);
		}

		PageIndices pageIndices = extractor.extractPageIndices(Paths.get(getClass().getResource("/documents/ocr/embedded.pdf").getPath()));

		assertThat(pageIndices).isNotNull();
		assertThat(pageIndices.pages()).isEqualTo(List.of(Pair.of(0L, 16L), Pair.of(17L,33L)));
		assertThat(text).hasSize(33 + 1);

		String expectedPage = """
		
		HEAVY
		METAL
		
		
		
		""";
		assertThat(getPage(pageIndices.pages().get(0), text)).isEqualTo(expectedPage);
		assertThat(getPage(pageIndices.pages().get(1), text)).isEqualTo(expectedPage);
	}

	@Test
	public void testPageExtractionForPdf() throws Exception {
        Extractor extractor = new Extractor();
		List<String> pages = extractor.extractPages(Paths.get(getClass().getResource("/documents/ocr/embedded.pdf").getPath()));

		assertThat(pages).hasSize(2);
		String expectedPage = """
		
		HEAVY
		METAL
		
		
		
		""";
		assertThat(pages.get(0)).isEqualTo(expectedPage);
		assertThat(pages.get(1)).isEqualTo(expectedPage);
	}

	@Test
	public void testPageExtractionForEmbeddedPdf() throws Exception {
        Extractor extractor = new Extractor();
		TikaDocument doc = extractor.extract(Paths.get(getClass().getResource("/documents/ocr/embedded_doc.eml").getPath()));
		extractor.setEmbedHandling(Extractor.EmbedHandling.SPAWN);

		try (final Reader reader = doc.getReader()) {
			Spewer.toString(reader);
		}
		EmbeddedTikaDocument embeddedTikaDocument = doc.getEmbeds().get(0);
		try (final Reader reader = embeddedTikaDocument.getReader()) {
			Spewer.toString(reader);
		}

		PageIndices pageIndices = extractor.extractPageIndices(
				Paths.get(getClass().getResource("/documents/ocr/embedded_doc.eml").getPath()),
				metadata -> "embedded.pdf".equals(metadata.get("resourceName")) || "INLINE".equals(metadata.get("embeddedResourceType")));

        assertThat(doc.getMetadata().get(OCR_PARSER)).isNull(); // no OCR_used on the eml root.
        assertThat(embeddedTikaDocument.getMetadata().get(OCR_PARSER)).isNotNull(); // but the pdf children has been OCRed
		assertThat(pageIndices).isNotNull();
		assertThat(pageIndices.pages()).isEqualTo(List.of(Pair.of(0L, 16L), Pair.of(17L,33L)));
	}

	@Test
	public void testPageExtractionForEmbeddedPdfWithRootDocument() throws Exception {
        Extractor extractor = new Extractor();
		TikaDocument doc = extractor.extract(Paths.get(getClass().getResource("/documents/ocr/embedded_doc.eml").getPath()));
		extractor.setEmbedHandling(Extractor.EmbedHandling.SPAWN);

		// we do not filter
		PageIndices pageIndices = extractor.extractPageIndices(
				Paths.get(getClass().getResource("/documents/ocr/embedded_doc.eml").getPath()));

		assertThat(pageIndices).isNotNull();
		assertThat(pageIndices.pages()).isEqualTo(List.of(Pair.of(0L, 109L)));
	}

	@Test
	public void testPageExtractionForEmbeddedPdfWithFilter() throws Exception {
        Extractor extractor = new Extractor();
		TikaDocument doc = extractor.extract(Paths.get(getClass().getResource("/documents/ocr/embedded_doc.eml").getPath()));
		extractor.setEmbedHandling(Extractor.EmbedHandling.SPAWN);

		// we do filter
		PageIndices pageIndices = extractor.extractPageIndices(
				Paths.get(getClass().getResource("/documents/ocr/embedded_doc.eml").getPath()),
				metadata -> "embedded.pdf".equals(metadata.get("resourceName")) || "INLINE".equals(metadata.get("embeddedResourceType")));

		assertThat(pageIndices).isNotNull();
		assertThat(pageIndices.pages()).isEqualTo(List.of(Pair.of(0L, 16L), Pair.of(17L,33L)));
	}

	@Test
	public void testIgnoreCacheForPageExtraction() throws Exception {
        Extractor extractor = new Extractor();
		PageIndices pageIndices = extractor.extractPageIndices(
				Paths.get(getClass().getResource("/documents/ocr/embedded_doc.eml").getPath()),
				metadata -> "embedded.pdf".equals(metadata.get("resourceName")) || "INLINE".equals(metadata.get("embeddedResourceType")), "embedded_id");

		assertThat(pageIndices).isNotNull();
	}

	@Test
	public void testArtifactCacheWriteForPageExtraction() throws Exception {
        Extractor extractor = new Extractor();
		extractor.setEmbedOutputPath(folder.getRoot().toPath());
		Path cachedPagesFile = ArtifactUtils.getEmbeddedPath(folder.getRoot().toPath(), "embedded_id").resolve("pages.json");
		PageIndices pageIndices = extractor.extractPageIndices(
				Paths.get(getClass().getResource("/documents/ocr/embedded_doc.eml").getPath()),
				metadata -> "embedded.pdf".equals(metadata.get("resourceName")) || "INLINE".equals(metadata.get("embeddedResourceType")), "embedded_id");

		assertThat(pageIndices).isNotNull();
		assertThat(cachedPagesFile.toFile()).exists();
		assertThat(new ObjectMapper().readValue(cachedPagesFile.toFile(), PageIndices.class)).isEqualTo(pageIndices);
	}

	@Test
	public void testArtifactCacheReadForPageExtraction() throws Exception {
        Extractor extractor = new Extractor();
		extractor.setEmbedOutputPath(folder.getRoot().toPath());
		String embeddedId = "embedded_id";
		Path cachedPagesFile = ArtifactUtils.getEmbeddedPath(folder.getRoot().toPath(), embeddedId).resolve("pages.json");
		Files.createDirectories(cachedPagesFile.getParent());
		Files.write(cachedPagesFile, """
				{
				   "extractor": "Tika 3.0.1",
				   "pages": [
				      [0, 123],
				      [124, 432]
				   ]
				}
		""".getBytes());

		PageIndices pageIndices = extractor.extractPageIndices(
				Paths.get(getClass().getResource("/documents/ocr/embedded_doc.eml").getPath()),
				metadata -> "embedded.pdf".equals(metadata.get("resourceName")) || "INLINE".equals(metadata.get("embeddedResourceType")), embeddedId);

		assertThat(pageIndices).isNotNull();
		assertThat(pageIndices.pages()).hasSize(2);
		assertThat(pageIndices.extractor()).isEqualTo("Tika 3.0.1");
	}

	@Test
	public void testOcrTypeFromOption() {
		Options<String> options = Options.from(Map.of("ocrType", "TESS4J"));
        Extractor extractor = new Extractor(options);

		assertThat(extractor.ocrConfig.getClass()).isEqualTo(Tess4JOCRConfigAdapter.class);
		assertThat(extractor.ocrConfig.getParserClass()).isEqualTo(Tess4JOCRParser.class);
	}

	private String getExpected(final String file) throws IOException {
		try (final Reader input = new InputStreamReader(getClass().getResourceAsStream(file), StandardCharsets.UTF_8)) {
			return Spewer.toString(input);
		}
	}

	private String getPage(Pair<Long, Long> startEndIndices, String fullText) {
		return fullText.substring(toIntExact(startEndIndices.getLeft()), toIntExact(startEndIndices.getRight()));
	}

}
