package org.icij.extract.extractor;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.tika.Tika;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.EncryptedDocumentException;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.CompositeParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.EmbeddedTikaDocument;
import org.icij.extract.document.PathIdentifier;
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
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.tika.parser.pdf.PDFParserConfig;

import static java.lang.Math.toIntExact;
import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assume.assumeTrue;
import static org.icij.extract.ocr.OCRParser.OCR_PARSER;
import static org.icij.extract.ocr.ParserWithConfidence.OCR_CONFIDENCE;

public class ExtractorTest {
	@Rule public final ExpectedException thrown = ExpectedException.none();
	@Rule public final TemporaryFolder folder = new TemporaryFolder();

	@Test
	public void testTikaVersion() throws Throwable {
        //GIVEN
        Extractor extractor = aBasicExtractor();
        //WHEN
		TikaDocument tikaDocument = extractDocument(extractor, "/documents/ocr/simple.tiff");
        //THEN
		Assert.assertEquals(Tika.getString(), tikaDocument.getMetadata().get(TikaDocument.TIKA_VERSION));
	}

    @Test
	public void testOcr() throws Throwable {
        //GIVEN
		String text;
        Extractor extractor = aBasicExtractor();
        //WHEN
		TikaDocument tikaDocument = extractDocument(extractor, "/documents/ocr/simple.tiff");
		try (Reader reader = tikaDocument.getReader()) {
			text = Spewer.toString(reader);
		}
        //THEN
        assertThat(tikaDocument.getMetadata().get(OCR_PARSER)).isNotNull();
        assertThat(tikaDocument.getMetadata().get(OCR_PARSER)).isEqualTo("org.apache.tika.parser.ocr.TesseractOCRParser");
		Assert.assertEquals("image/ocr-tiff", tikaDocument.getMetadata().get(Metadata.CONTENT_TYPE));
		Assert.assertEquals("HEAVY\nMETAL", text.trim());
	}

	@Test
	public void testExtractorShouldSupportMultipleLanguage() throws Throwable {
        //GIVEN
        String text;
        Extractor extractor = aBasicExtractor();
        extractor.setOcrLanguage("eng+spa");
        //WHEN
		TikaDocument tikaDocument = extractDocument(extractor, "/documents/ocr/simple.tiff");
		try (Reader reader = tikaDocument.getReader()) {
			text = Spewer.toString(reader);
		}
        //THEN
		Assert.assertEquals("image/ocr-tiff", tikaDocument.getMetadata().get(Metadata.CONTENT_TYPE));
		Assert.assertEquals("HEAVY\nMETAL", text.trim());
		// We expect a null language, downstream component will be responsible to guess the language from the content
		assertThat(tikaDocument.getLanguage()).isNull();
	}

	@Test
	public void testDisableOcr() throws Throwable {
        //GIVEN
        Extractor extractor = aBasicExtractor();
		extractor.disableOcr();
        //WHEN
		TikaDocument tikaDocument = extractDocument(extractor, "/documents/ocr/simple.tiff");
		final int read = tikaDocument.getReader().read();
        //THEN
		Assert.assertEquals("image/tiff", tikaDocument.getMetadata().get(Metadata.CONTENT_TYPE));
		Assert.assertEquals(-1, read);
        assertThat(tikaDocument.getMetadata().get(OCR_PARSER)).isNull();
    }

	@Test
	public void testCSVFile() throws Throwable {
        //GIVEN
        String text;
        Extractor extractor = aBasicExtractor();
        //WHEN
        TikaDocument tikaDocument = extractDocument(extractor, "/documents/csv_document.csv");
		try (Reader reader = tikaDocument.getReader()) {
			text = Spewer.toString(reader);
		}
        //THEN
		Assert.assertEquals("text/csv; charset=UTF-8; delimiter=comma", tikaDocument.getMetadata().get(Metadata.CONTENT_TYPE));
		Assert.assertEquals(12, text.lines().count());
	}

	@Test
	public void testRtfFile() throws Throwable {
        //GIVEN
        String text;
        Extractor extractor = aBasicExtractor();
        //WHEN
        TikaDocument tikaDocument = extractDocument(extractor, "/documents/text/doc.rtf");
		try (Reader reader = tikaDocument.getReader()) {
			text = Spewer.toString(reader);
		}
        //WHEN
		Assert.assertEquals("application/rtf", tikaDocument.getMetadata().get(Metadata.CONTENT_TYPE));
		Assert.assertEquals("RTF Text Document", text.trim());
	}

	@Test
	public void testFileNotFound() throws Throwable {
        //GIVEN,*THEN
        Extractor extractor = aBasicExtractor();
		thrown.expect(NoSuchFileException.class);
		thrown.expectMessage("nothing");
        //WHEN
		extractor.extract(Paths.get("nothing"));
	}

	@Test
	public void testEncryptedPdf() throws Throwable {
        //GIVEN,*THEN
        final int read;
        Extractor extractor = aBasicExtractor();
        thrown.expect(IOException.class);
        thrown.expectMessage("");
        thrown.expectCause(new CauseMatcher(EncryptedDocumentException.class, "Unable to process: document is encrypted"));
        //WHEN
        TikaDocument tikaDocument = extractDocument(extractor, "/documents/pdf/encrypted.pdf");
		try (final Reader reader = tikaDocument.getReader()) {
			read = reader.read();
		} catch (IOException e) {
            //THEN
			Assert.assertEquals("application/pdf", tikaDocument.getMetadata().get(Metadata.CONTENT_TYPE));
			throw e;
		}

		Assert.fail(String.format("Read \"%d\" while expecting exception.", read));
	}

	@Test
	public void testGarbage() throws Throwable {
        //GIVEN
        Extractor extractor = aBasicExtractor();
        //WHEN
        TikaDocument tikaDocument = extractDocument(extractor, "/documents/garbage.bin");
        final String content;
        try (final Reader reader = tikaDocument.getReader()) {
            content = Spewer.toString(reader);
        }
        //THEN: indexed as empty, marked unsupported, no exception
        assertThat(content.trim()).isEmpty();
        Assert.assertEquals("application/octet-stream", tikaDocument.getMetadata().get(Metadata.CONTENT_TYPE));
        assertThat(tikaDocument.getMetadata().get(NoContentReason.METADATA_KEY)).isEqualTo("unsupported-media-type");
	}

	@Test
	public void testByProjectDigester() throws Exception {
        //GIVEN
		DocumentFactory documentFactory = new DocumentFactory().configure(Options.from(Map.of("digestAlgorithm", "SHA-384")));
        //WHEN
		TikaDocument tikaDocument1 = new Extractor(documentFactory, Options.from(Map.of("digestAlgorithm", "SHA-384")))
            .extract(Paths.get(getClass().getResource("/documents/ocr/simple.tiff").getPath()));
		TikaDocument tikaDocument2 = new Extractor(documentFactory, Options.from(Map.of("digestAlgorithm", "SHA-384", "digestProjectName", "project1")))
            .extract(Paths.get(getClass().getResource("/documents/ocr/simple.tiff").getPath()));
		TikaDocument tikaDocument3 = new Extractor(documentFactory, Options.from(Map.of("digestAlgorithm", "SHA-384", "digestProjectName", "project2")))
            .extract(Paths.get(getClass().getResource("/documents/ocr/simple.tiff").getPath()));
        //THEN
		Assert.assertNotEquals(tikaDocument1.getId(), tikaDocument2.getId());
		Assert.assertNotEquals(tikaDocument1.getId(), tikaDocument3.getId());
		Assert.assertNotEquals(tikaDocument2.getId(), tikaDocument3.getId());
	}

    @Test
	public void testDocumentUseCorrectDigestIdentifier () throws Exception {
        //GIVEN
		Options<String> digestAlgorithm = Options.from(Map.of("digestAlgorithm", "SHA-384", "digestProjectName", "project1"));
		DocumentFactory documentFactory = new DocumentFactory().configure(digestAlgorithm);
		final Extractor extractor = new Extractor(documentFactory, digestAlgorithm);
        //WHEN
		TikaDocument tikaDocument1 = extractDocument(extractor, "/documents/ocr/simple.tiff");
        //THEN
		Assert.assertNotNull(tikaDocument1.getId());
	}

	@Test
	public void testDocumentUseCorrectLanguage () throws IOException {
        //GIVEN
        Options<String> options = Options.from(Map.of("language", "zho"));
		DocumentFactory documentFactory = new DocumentFactory().configure(options);
        final Extractor extractor = new Extractor(documentFactory, options);
        //WHEN
		TikaDocument tikaDocument1 = extractDocument(extractor, "/documents/ocr/simple.tiff");
        //THEN
		Assert.assertEquals(tikaDocument1.getLanguage(), "zho");
	}

	@Test
	public void testDocumentHasNoLanguage () throws IOException {
        //GIVEN
		DocumentFactory documentFactory = new DocumentFactory().configure();
		final Extractor extractor = new Extractor(documentFactory);
        //WHEN
		TikaDocument tikaDocument1 = extractDocument(extractor, "/documents/ocr/simple.tiff");
        //THEN
		Assert.assertNull(tikaDocument1.getLanguage());
	}

	@Test
	public void testEmbeds() throws Throwable {
        //GIVEN
        String text;
        //WHEN
        TikaDocument tikaDocument = extractDocument(aBasicExtractor(), "/documents/ocr/embedded.pdf");
		try (final Reader reader = tikaDocument.getReader()) {
			text = Spewer.toString(reader);
		}
        //THEN
        assertThat(tikaDocument.getMetadata().get(OCR_PARSER)).isNotNull();
		Assert.assertEquals("application/pdf", tikaDocument.getMetadata().get(Metadata.CONTENT_TYPE));
		Assert.assertThat(text, RegexMatcher.matchesRegex("^\\s+HEAVY\\sMETAL\\s+HEAVY\\sMETAL\\s+$"));
	}

	@Test
	public void testIgnoreEmbeds() throws Throwable {
        //GIVEN
        String text;
        Extractor extractor = aBasicExtractor();
        extractor.setEmbedHandling(Extractor.EmbedHandling.IGNORE);
        Assert.assertEquals(extractor.getEmbedHandling(), Extractor.EmbedHandling.IGNORE);
        //WHEN
        TikaDocument tikaDocument = extractor.extract(Paths.get(getClass().getResource("/documents/ocr/embedded.pdf").getPath()));
		try (final Reader reader = tikaDocument.getReader()) {
			text = Spewer.toString(reader);
		}
        //THEN
        assertThat(tikaDocument.getMetadata().get(OCR_PARSER)).isNull();
		Assert.assertEquals("application/pdf", tikaDocument.getMetadata().get(Metadata.CONTENT_TYPE));
		Assert.assertEquals("\n\n\n\n", text);
	}

	@Test
	public void testDisableOcrOnEmbed() throws Throwable {
        //GIVEN
        String text;
        Extractor extractor = aBasicExtractor();
        extractor.disableOcr();
        //WHEN
        TikaDocument tikaDocument = extractor.extract(Paths.get(getClass().getResource("/documents/ocr/embedded.pdf").getPath()));
		try (final Reader reader = tikaDocument.getReader()) {
			text = Spewer.toString(reader);
		}
        //THEN
        assertThat(tikaDocument.getMetadata().get(OCR_PARSER)).isNull();
		Assert.assertEquals("application/pdf", tikaDocument.getMetadata().get(Metadata.CONTENT_TYPE));
		Assert.assertEquals("\n\n\n\n", text);
	}

	@Test
	public void testHtmlOutput() throws Throwable {
        //GIVEN
        String text;
        Extractor extractor = aBasicExtractor();
        extractor.setOutputFormat(Extractor.OutputFormat.HTML);
        //WHEN
        TikaDocument tikaDocument = extractor.extract(Paths.get(getClass().getResource("/documents/text/utf16.txt").getPath()));
		try (final Reader reader = tikaDocument.getReader()) {
			text = Spewer.toString(reader);
		}
        //THEN
		Assert.assertEquals("text/plain; charset=UTF-16LE", tikaDocument.getMetadata().get(Metadata.CONTENT_TYPE));
		Assert.assertEquals(getExpected("/expected/utf16-txt.html"), text);
	}

	@Test
	public void test_ocr_confidence() throws Exception {
        //GIVEN
        Extractor extractor = new Extractor(Options.from(Map.of("ocrType", "TESS4J")));
        //WHEN
		TikaDocument tikaDocument = extractor.extract(Paths.get(getClass().getResource("/documents/ocr/test.jpeg").getPath()));
        //THEN
        assertThat(tikaDocument.getMetadata().get(OCR_PARSER)).isEqualTo("org.icij.extract.ocr.Tess4JOCRParser");
		assertThat(Optional.ofNullable(tikaDocument.getMetadata().get(OCR_CONFIDENCE)).map(Float::parseFloat).orElse(0.0f))
			.isGreaterThan(0.0f);
	}

	@Test
	public void testHtmlOutputWithEmbeddedEmbeds() throws Throwable {
        //GIVEN
        String text;
        Extractor extractor = aBasicExtractor();
        extractor.setOutputFormat(Extractor.OutputFormat.HTML);
        Assert.assertEquals(extractor.getOutputFormat(), Extractor.OutputFormat.HTML);
        //WHEN
        TikaDocument tikaDocument = extractor.extract(Paths.get(getClass().getResource("/documents/ocr/embedded.pdf").getPath()));
		try (final Reader reader = tikaDocument.getReader()) {
			text = Spewer.toString(reader);
		}
        //THEN
        assertThat(tikaDocument.getMetadata().get(OCR_PARSER)).isNotNull();
		Assert.assertEquals("application/pdf", tikaDocument.getMetadata().get(Metadata.CONTENT_TYPE));
		Assert.assertEquals(getExpected("/expected/embedded-data-uri-pdf.html"), text);
	}

	@Test
	public void testRecursiveEmbedded() throws Exception {
        //GIVEN
        Extractor extractor = aBasicExtractor();
		TikaDocument tikaDocument = extractor.extract(Paths.get(getClass().getResource("/documents/recursive_embedded.docx").getPath()));

		/* we have to use a spewer and not just extract and check the structure of the tikaDocument
		 because the principle is to stream the embedded documents to avoid mounting all the documents in memory.
		 That could be an issue for big documents like mailboxes, zips or tarballs.

		 see https://cwiki.apache.org/confluence/display/tika/RecursiveMetadata#Jukka.27s_RecursiveMetadata_Parser
		 "A downside to the wrapper is that it breaks the Tika goal of streaming output"

		 so we just use a print stream spewer and check that all the tree has been parsed */
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        //WHEN
		new PrintStreamSpewer(new PrintStream(outputStream), new FieldNames()).write(tikaDocument);
        //THEN
		String allContents = outputStream.toString();

		assertThat(allContents).contains("embed_0")
                               .contains("embed_1a")
                               .contains("embed_1b")
                               .contains("embed_2a")
                               .contains("embed_2b")
                               .contains("embed_3")
                               .contains("dissolve the political bands")
                               .contains("embed_4");
	}

	@Test
	public void testEmbedTextSpillFilesAreCleanedUpAfterSpew() throws Exception {
		// GIVEN an extractor forced to spill every embedded document's text to disk
		Extractor extractor = aBasicExtractor();
		extractor.disableOcr();
		extractor.setEmbedOutputPath(folder.newFolder("embeds").toPath());
		extractor.setEmbedMemoryBudgetBytes(0L); // 0 budget => first embed write spills to a temp file

		Set<Path> before = tikaTempFiles();

		// WHEN extracting through the spewer path (as DocumentConsumer / Datashare do)
		extractor.extract(
				Paths.get(getClass().getResource("/documents/recursive_embedded.docx").getPath()),
				new PrintStreamSpewer(new PrintStream(OutputStream.nullOutputStream()), new FieldNames()));

		// THEN the spilled temp files do not leak once the extraction is finished
		Set<Path> leaked = tikaTempFiles();
		leaked.removeAll(before);
		assertThat(new ArrayList<>(leaked)).isEmpty();
	}

	private static Set<Path> tikaTempFiles() throws IOException {
		Path tmp = Paths.get(System.getProperty("java.io.tmpdir"));
		try (Stream<Path> files = Files.list(tmp)) {
			return files.filter(p -> p.getFileName().toString().startsWith("apache-tika-"))
					.collect(Collectors.toSet());
		}
	}

	@Test
	public void testSpilledEmbedReadersDoNotLeakFileDescriptors() throws Exception {
		// Each spilled embed's reader is an open file handle; if the spew walk doesn't close them
		// they stay open (cached on each TikaDocument) until GC, exhausting the fd limit on large
		// containers. Count fds pointing at apache-tika temp files before vs after a forced-spill spew.
		assumeTrue("requires /proc/self/fd (Linux)", Files.isDirectory(Paths.get("/proc/self/fd")));
		Extractor extractor = aBasicExtractor();
		extractor.disableOcr();
		extractor.setEmbedOutputPath(folder.newFolder("embeds").toPath());
		extractor.setEmbedMemoryBudgetBytes(0L); // force every embed to spill

		long before = openTikaTempFds();
		extractor.extract(
				Paths.get(getClass().getResource("/documents/recursive_embedded.docx").getPath()),
				new PrintStreamSpewer(new PrintStream(OutputStream.nullOutputStream()), new FieldNames()));
		long after = openTikaTempFds();

		assertThat(after).isEqualTo(before);
	}

	private static long openTikaTempFds() throws IOException {
		try (Stream<Path> fds = Files.list(Paths.get("/proc/self/fd"))) {
			return fds.filter(fd -> {
				try {
					return Files.readSymbolicLink(fd).toString().contains("apache-tika");
				} catch (IOException e) {
					return false;
				}
			}).count();
		}
	}

	@Test
	public void testEmbeddedWithDuplicates() throws Exception {
        //GIVEN
        Extractor extractor = aBasicExtractor();
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
        //WHEN
		TikaDocument tikaDocument = extractDocument(extractor, "/documents/embedded_with_duplicate.tgz");
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
        //THEN
		assertThat(folder.getRoot().toPath().resolve("embeds").toFile().listFiles()).hasSize(6);
	}

	@Test
	public void testPageIndicesExtractionForPdf() throws Exception {
        //GIVEN
        String text;
        Extractor extractor = aBasicExtractor();
        //WHEN
        TikaDocument doc = extractDocument(extractor, "/documents/ocr/embedded.pdf");
		try (final Reader reader = doc.getReader()) {
			text = Spewer.toString(reader);
		}
		PageIndices pageIndices = extractor.extractPageIndices(Paths.get(getClass().getResource("/documents/ocr/embedded.pdf").getPath()));
        //THEN
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
        //GIVEN
        Extractor extractor = aBasicExtractor();
        //WHEN
		List<String> pages = extractor.extractPages(Paths.get(getClass().getResource("/documents/ocr/embedded.pdf").getPath()));
        //THEN
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
        //GIVEN
        Extractor extractor = aBasicExtractor();
        extractor.setEmbedHandling(Extractor.EmbedHandling.SPAWN);
        //WHEN
        TikaDocument doc = extractDocument(extractor, "/documents/ocr/embedded_doc.eml");

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
        //THEN
        assertThat(doc.getMetadata().get(OCR_PARSER)).isNull(); // no OCR_used on the eml root.
        assertThat(embeddedTikaDocument.getMetadata().get(OCR_PARSER)).isNotNull(); // but the pdf children has been OCRed
		assertThat(pageIndices).isNotNull();
		assertThat(pageIndices.pages()).isEqualTo(List.of(Pair.of(0L, 16L), Pair.of(17L,33L)));
	}

	@Test
	public void testPageExtractionForEmbeddedPdfWithRootDocument() throws Exception {
        //GIVEN
        Extractor extractor = aBasicExtractor();
        extractor.setEmbedHandling(Extractor.EmbedHandling.SPAWN);
        //WHEN
        // we do not filter
		PageIndices pageIndices = extractor.extractPageIndices(
				Paths.get(getClass().getResource("/documents/ocr/embedded_doc.eml").getPath()));
        //THEN
		assertThat(pageIndices).isNotNull();
		assertThat(pageIndices.pages()).isEqualTo(List.of(Pair.of(0L, 109L)));
	}

	@Test
	public void testPageExtractionForEmbeddedPdfWithFilter() throws Exception {
        //GIVEN
        Extractor extractor = aBasicExtractor();
		extractor.setEmbedHandling(Extractor.EmbedHandling.SPAWN);
        //WHEN
		// we do filter
		PageIndices pageIndices = extractor.extractPageIndices(
				Paths.get(getClass().getResource("/documents/ocr/embedded_doc.eml").getPath()),
				metadata -> "embedded.pdf".equals(metadata.get("resourceName")) || "INLINE".equals(metadata.get("embeddedResourceType")));
        //THEN
		assertThat(pageIndices).isNotNull();
		assertThat(pageIndices.pages()).isEqualTo(List.of(Pair.of(0L, 16L), Pair.of(17L,33L)));
	}

	@Test
	public void testIgnoreCacheForPageExtraction() throws Exception {
        //GIVEN
        Extractor extractor = aBasicExtractor();
        //WHEN
		PageIndices pageIndices = extractor.extractPageIndices(
				Paths.get(getClass().getResource("/documents/ocr/embedded_doc.eml").getPath()),
				metadata -> "embedded.pdf".equals(metadata.get("resourceName")) || "INLINE".equals(metadata.get("embeddedResourceType")), "embedded_id");
        //THEN
		assertThat(pageIndices).isNotNull();
	}

	@Test
	public void testArtifactCacheWriteForPageExtraction() throws Exception {
        //GIVEN
        Extractor extractor = aBasicExtractor();
		extractor.setEmbedOutputPath(folder.getRoot().toPath());
        //WHEN
		Path cachedPagesFile = ArtifactUtils.getEmbeddedPath(folder.getRoot().toPath(), "embedded_id").resolve("pages.json");
		PageIndices pageIndices = extractor.extractPageIndices(
				Paths.get(getClass().getResource("/documents/ocr/embedded_doc.eml").getPath()),
				metadata -> "embedded.pdf".equals(metadata.get("resourceName")) || "INLINE".equals(metadata.get("embeddedResourceType")), "embedded_id");
        //THEN
		assertThat(pageIndices).isNotNull();
		assertThat(cachedPagesFile.toFile()).exists();
		assertThat(new ObjectMapper().readValue(cachedPagesFile.toFile(), PageIndices.class)).isEqualTo(pageIndices);
	}

	@Test
	public void testArtifactCacheReadForPageExtraction() throws Exception {
        //GIVEN
        String embeddedId = "embedded_id";
        Extractor extractor = aBasicExtractor();
        extractor.setEmbedOutputPath(folder.getRoot().toPath());
        //WHEN
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
        //THEN
		assertThat(pageIndices).isNotNull();
		assertThat(pageIndices.pages()).hasSize(2);
		assertThat(pageIndices.extractor()).isEqualTo("Tika 3.0.1");
	}

	@Test
	public void testOcrTypeFromOption() {
        //GIVEN
		Options<String> options = Options.from(Map.of("ocrType", "TESS4J"));
        Extractor extractor = new Extractor(options);
        //THEN
		assertThat(extractor.ocrConfig.getClass()).isEqualTo(Tess4JOCRConfigAdapter.class);
		assertThat(extractor.ocrConfig.getParserClass()).isEqualTo(Tess4JOCRParser.class);
	}

	@Test
	public void testEmbedMemoryBudgetDefaultsTo64Mb() {
		Extractor extractor = new Extractor();
		assertThat(extractor.getEmbedMemoryBudgetBytes()).isEqualTo(64L * 1024 * 1024);
	}

	@Test
	public void testEmbedMemoryPressureThresholdDefaultsTo07() {
		Extractor extractor = new Extractor();
		assertThat(extractor.getEmbedMemoryPressureThreshold()).isEqualTo(0.7);
	}

	@Test
	public void testEmbedMemoryPressureThresholdOptionParsed() {
		Extractor extractor = new Extractor(Options.from(Map.of("embedMemoryPressureThreshold", "0.5")));
		assertThat(extractor.getEmbedMemoryPressureThreshold()).isEqualTo(0.5);
	}

	@Test
	public void testEmbedMemoryBudgetOptionParsedFromMegabytes() {
		Options<String> options = Options.from(Map.of("embedMemoryBudgetMb", "2"));
		Extractor extractor = new Extractor(options);
		assertThat(extractor.getEmbedMemoryBudgetBytes()).isEqualTo(2L * 1024 * 1024);
	}

    @Test public void testMaxEmbedDepthDefaultsTo20() {
        Extractor extractor = new Extractor(Options.from(java.util.Map.of()));
        assertThat(extractor.getMaxEmbedDepth()).isEqualTo(20);
    }

    @Test public void testMaxEmbedDepthReadsOption() {
        Extractor extractor = new Extractor(Options.from(java.util.Map.of("maxEmbedDepth", "8")));
        assertThat(extractor.getMaxEmbedDepth()).isEqualTo(8);
    }

    @Test public void testMaxEmbedDepthZeroDisables() {
        Extractor extractor = new Extractor(Options.from(java.util.Map.of("maxEmbedDepth", "0")));
        assertThat(extractor.getMaxEmbedDepth()).isEqualTo(0);
    }

    @Test public void testMaxEmbedDepthClampsNegativeToZero() {
        Extractor extractor = new Extractor(Options.from(java.util.Map.of("maxEmbedDepth", "-3")));
        assertThat(extractor.getMaxEmbedDepth()).isEqualTo(0);
    }

	@Test
	public void testEmbedTextIsIdenticalWhetherSpilledOrInMemory() throws Exception {
		String inMemory = spewTreeText(512L * 1024 * 1024); // large budget: nothing spills
		String spilled = spewTreeText(1L);                  // 1-byte budget: everything spills

		assertThat(spilled).isEqualTo(inMemory);
		assertThat(spilled).contains("level1");             // sanity: real embed content present
	}

	@Test
	public void testOcrShouldExtractTextFromJbig2ScannedPdf() throws Throwable {
	    //GIVEN
	    String text;
	    Extractor extractor = aBasicExtractor();
	    //WHEN
	    TikaDocument tikaDocument = extractDocument(extractor, "/documents/ocr/jbig2_scan.pdf");
	    try (Reader reader = tikaDocument.getReader()) {
	        text = Spewer.toString(reader);
	    }
	    //THEN
	    assertThat(tikaDocument.getMetadata().get(OCR_PARSER)).isNotNull();
	    Assert.assertEquals("application/pdf", tikaDocument.getMetadata().get(Metadata.CONTENT_TYPE));
	    assertThat(text).contains("Lorem ipsum");
	    assertThat(text).contains("1234567890");
	}

	@Test
	public void testDisableOcrLeavesJbig2ScannedPdfEmpty() throws Throwable {
	    //GIVEN
	    String text;
	    Extractor extractor = aBasicExtractor();
	    extractor.disableOcr();
	    //WHEN
	    TikaDocument tikaDocument = extractDocument(extractor, "/documents/ocr/jbig2_scan.pdf");
	    try (Reader reader = tikaDocument.getReader()) {
	        text = Spewer.toString(reader);
	    }
	    //THEN
	    // PDF page structure always emits newlines even for image-only pages, so check blank not -1
	    assertThat(text.trim()).isEmpty();
	    assertThat(tikaDocument.getMetadata().get(OCR_PARSER)).isNull();
	}

	@Test
	public void testOcrStrategyAutoRendersPagesAndExtractsText() throws Throwable {
	    //GIVEN
	    String text;
	    Extractor extractor = aBasicExtractor();
	    extractor.setOcrStrategy("AUTO");
	    //WHEN
	    TikaDocument tikaDocument = extractDocument(extractor, "/documents/ocr/jbig2_scan.pdf");
	    try (Reader reader = tikaDocument.getReader()) {
	        text = Spewer.toString(reader);
	    }
	    //THEN: with a rendering strategy, inline-image extraction is off and Tika renders whole
	    // pages for OCR, recovering the scanned text across both pages.
	    assertThat(text).contains("Lorem ipsum");
	    assertThat(text).contains("1234567890");
	}

	@Test
	public void testAddParserWinsForItsMediaTypes() throws Exception {
	    //GIVEN
	    CompositeParser base = (CompositeParser) TikaConfig.getDefaultConfig().getParser();
	    Parser custom = new Parser() {
	        @Override public Set<MediaType> getSupportedTypes(ParseContext c) {
	            return Set.of(MediaType.parse("image/x-jbig2"));
	        }
	        @Override public void parse(java.io.InputStream s, org.xml.sax.ContentHandler h,
	                                    org.apache.tika.metadata.Metadata m, ParseContext c) {}
	    };
	    //WHEN
	    CompositeParser result = Extractor.addParser(base, custom);
	    //THEN
	    assertThat(result.getParsers(new ParseContext()).get(MediaType.parse("image/x-jbig2")))
	            .isSameAs(custom);
	}

	@Test
	public void testNormalArchiveDoesNotSpillUnderDefaultBudget() throws Exception {
		Extractor extractor = aBasicExtractor();
		extractor.disableOcr();
		// default budget: a tiny archive must read back fine with no error
		TikaDocument doc = extractDocument(extractor, "/documents/embedded_with_duplicate.tgz");
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		new PrintStreamSpewer(new PrintStream(out, true, StandardCharsets.UTF_8.name()), new FieldNames()).write(doc);
		doc.getReader().close();
		assertThat(out.toString(StandardCharsets.UTF_8)).contains("level1");
	}

	private String spewTreeText(final long budgetBytes) throws Exception {
		Extractor extractor = aBasicExtractor();
		extractor.disableOcr();
		extractor.setEmbedMemoryBudgetBytes(budgetBytes);

		TikaDocument doc = extractDocument(extractor, "/documents/embedded_with_duplicate.tgz");
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		PrintStreamSpewer spewer = new PrintStreamSpewer(new PrintStream(out, true, StandardCharsets.UTF_8.name()), new FieldNames());
		spewer.outputMetadata(false); // exclude non-deterministic metadata (temp file names, timestamps)
		spewer.write(doc);
		doc.getReader().close(); // triggers ResourceClosingReader cleanup of any temp files
		return out.toString(StandardCharsets.UTF_8);
	}

	private String getExpected(final String file) throws IOException {
		try (final Reader input = new InputStreamReader(getClass().getResourceAsStream(file), StandardCharsets.UTF_8)) {
			return Spewer.toString(input);
		}
	}

	private String getPage(Pair<Long, Long> startEndIndices, String fullText) {
		return fullText.substring(toIntExact(startEndIndices.getLeft()), toIntExact(startEndIndices.getRight()));
	}

	@Test
	public void testSetOcrStrategyDefaultsToNoOcrAndKeepsInlineImages() {
		Extractor extractor = new Extractor();
		assertThat(extractor.getOcrStrategy()).isEqualTo(PDFParserConfig.OCR_STRATEGY.NO_OCR);
		assertThat(extractor.isExtractInlineImages()).isTrue();
	}

	@Test
	public void testSetOcrStrategyAutoDisablesInlineImages() {
		Extractor extractor = new Extractor();
		extractor.setOcrStrategy("AUTO");
		assertThat(extractor.getOcrStrategy()).isEqualTo(PDFParserConfig.OCR_STRATEGY.AUTO);
		assertThat(extractor.isExtractInlineImages()).isFalse();
	}

	@Test
	public void testSetOcrStrategyIsCaseInsensitive() {
		Extractor extractor = new Extractor();
		extractor.setOcrStrategy("ocr_and_text_extraction");
		assertThat(extractor.getOcrStrategy())
				.isEqualTo(PDFParserConfig.OCR_STRATEGY.OCR_AND_TEXT_EXTRACTION);
		assertThat(extractor.isExtractInlineImages()).isFalse();
	}

	@Test
	public void testSetOcrStrategyInvalidFallsBackToNoOcr() {
		Extractor extractor = new Extractor();
		extractor.setOcrStrategy("AUTO");
		extractor.setOcrStrategy("not-a-strategy");
		assertThat(extractor.getOcrStrategy()).isEqualTo(PDFParserConfig.OCR_STRATEGY.NO_OCR);
		assertThat(extractor.isExtractInlineImages()).isTrue();
	}

	@Test
	public void testDisableOcrResetsRenderingStrategyToNoOcr() {
		Extractor extractor = new Extractor();
		extractor.setOcrStrategy("AUTO");
		extractor.disableOcr();
		// With the OCR parsers excluded a rendering strategy would only strip page text, so
		// disableOcr() must reset the strategy to NO_OCR to keep pdfConfig coherent.
		assertThat(extractor.getOcrStrategy()).isEqualTo(PDFParserConfig.OCR_STRATEGY.NO_OCR);
		assertThat(extractor.isExtractInlineImages()).isFalse();
	}

	@Test
	public void testConfigureOcrAndRenderingStrategyTogetherDisablesOcr() {
		// ocr=off must win over a rendering ocrStrategy: no rendering strategy may survive, or
		// born-digital PDFs would render with their text silently dropped.
		Extractor extractor = new Extractor(
				new DocumentFactory().withIdentifier(new PathIdentifier()),
				Options.from(Map.of("ocrStrategy", "OCR_ONLY", "ocr", "false")));
		assertThat(extractor.getOcrStrategy()).isEqualTo(PDFParserConfig.OCR_STRATEGY.NO_OCR);
		assertThat(extractor.isExtractInlineImages()).isFalse();
	}

	@Test
	public void testSetOcrStrategyAfterDisableOcrStaysNoOcr() {
		Extractor extractor = new Extractor();
		extractor.disableOcr();
		extractor.setOcrStrategy("AUTO");
		// Setting a rendering strategy on an OCR-disabled extractor must not re-enable inline
		// images nor activate page rendering.
		assertThat(extractor.getOcrStrategy()).isEqualTo(PDFParserConfig.OCR_STRATEGY.NO_OCR);
		assertThat(extractor.isExtractInlineImages()).isFalse();
	}

	@Test
	public void testConfigureOcrStrategyFromOptions() {
		Extractor extractor = new Extractor(
				new DocumentFactory().withIdentifier(new PathIdentifier()),
				Options.from(Map.of("ocrStrategy", "AUTO")));
		assertThat(extractor.getOcrStrategy()).isEqualTo(PDFParserConfig.OCR_STRATEGY.AUTO);
		assertThat(extractor.isExtractInlineImages()).isFalse();
	}

	@Test
	public void testConfigureOcrStrategyDefaultIsNoOcr() {
		Extractor extractor = new Extractor(
				new DocumentFactory().withIdentifier(new PathIdentifier()),
				Options.from(Map.of()));
		assertThat(extractor.getOcrStrategy()).isEqualTo(PDFParserConfig.OCR_STRATEGY.NO_OCR);
		assertThat(extractor.isExtractInlineImages()).isTrue();
	}

	@Test
	public void testSetOcrStrategyOcrOnlyDisablesInlineImages() {
		Extractor extractor = new Extractor();
		extractor.setOcrStrategy("OCR_ONLY");
		assertThat(extractor.getOcrStrategy()).isEqualTo(PDFParserConfig.OCR_STRATEGY.OCR_ONLY);
		assertThat(extractor.isExtractInlineImages()).isFalse();
	}

	@Test
	public void testSetOcrStrategyNullFallsBackToNoOcr() {
		Extractor extractor = new Extractor();
		extractor.setOcrStrategy(null);
		assertThat(extractor.getOcrStrategy()).isEqualTo(PDFParserConfig.OCR_STRATEGY.NO_OCR);
		assertThat(extractor.isExtractInlineImages()).isTrue();
	}

    public static Extractor aBasicExtractor() {
        return new Extractor();
    }

    private TikaDocument extractDocument(Extractor extractor, String stringPath) throws IOException {
        return extractor.extract(Paths.get(getClass().getResource(stringPath).getPath()));
    }
}
