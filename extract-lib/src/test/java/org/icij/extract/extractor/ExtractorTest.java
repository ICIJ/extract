package org.icij.extract.extractor;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tika.exception.EncryptedDocumentException;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.TikaDocument;
import org.icij.spewer.FieldNames;
import org.icij.spewer.FileSpewer;
import org.icij.spewer.PrintStreamSpewer;
import org.icij.spewer.Spewer;
import org.icij.task.Options;
import org.icij.test.CauseMatcher;
import org.icij.test.RegexMatcher;
import org.junit.Assert;
import org.junit.Before;
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
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;

import static java.lang.Math.toIntExact;
import static org.fest.assertions.Assertions.assertThat;

public class ExtractorTest {
	@Rule public final ExpectedException thrown = ExpectedException.none();
	@Rule public final TemporaryFolder folder = new TemporaryFolder();

	private Extractor extractor;

	@Before public void setUp() { extractor = new Extractor();}

	@Test
	public void testOcr() throws Throwable {
		String text;
		TikaDocument tikaDocument = extractor.extract(Paths.get(getClass().getResource("/documents/ocr/simple.tiff").getPath()));
		try (Reader reader = tikaDocument.getReader()) {
			text = Spewer.toString(reader);
		}

		Assert.assertEquals("image/tiff", tikaDocument.getMetadata().get(Metadata.CONTENT_TYPE));
		Assert.assertEquals("HEAVY\nMETAL", text.trim());
	}

	@Test
	public void testExtractorShouldSupportMultipleLanguage() throws Throwable {
		// When
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
		extractor.disableOcr();

		TikaDocument tikaDocument = extractor.extract(Paths.get(getClass().getResource("/documents/ocr/simple.tiff").getPath()));

		final int read = tikaDocument.getReader().read();

		Assert.assertEquals("image/tiff", tikaDocument.getMetadata().get(Metadata.CONTENT_TYPE));
		Assert.assertEquals(-1, read);
	}

	@Test
	public void testRtfFile() throws Throwable {
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
		thrown.expect(NoSuchFileException.class);
		thrown.expectMessage("nothing");

		extractor.extract(Paths.get("nothing"));
	}

	@Test
	public void testEncryptedPdf() throws Throwable {
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
		DocumentFactory documentFactory = new DocumentFactory().configure(Options.from(new HashMap<>() {{
			put("digestAlgorithm", "SHA-384");
		}}));
		final Extractor extractor = new Extractor(documentFactory);
		extractor.configure(Options.from(new HashMap<>() {{
			put("digestAlgorithm", "SHA-384");
		}}));
		TikaDocument tikaDocument1 = extractor.extract(Paths.get(getClass().getResource("/documents/ocr/simple.tiff").getPath()));
		extractor.configure(Options.from(new HashMap<>() {{
			put("digestAlgorithm", "SHA-384");
			put("digestProjectName", "project1");
		}}));
		TikaDocument tikaDocument2 = extractor.extract(Paths.get(getClass().getResource("/documents/ocr/simple.tiff").getPath()));
		extractor.configure(Options.from(new HashMap<>() {{
			put("digestAlgorithm", "SHA-384");
			put("digestProjectName", "project2");
		}}));
		TikaDocument tikaDocument3 = extractor.extract(Paths.get(getClass().getResource("/documents/ocr/simple.tiff").getPath()));

		Assert.assertNotEquals(tikaDocument1.getId(), tikaDocument2.getId());
		Assert.assertNotEquals(tikaDocument1.getId(), tikaDocument3.getId());
		Assert.assertNotEquals(tikaDocument2.getId(), tikaDocument3.getId());
	}

	@Test
	public void testDocumentUseCorrectDigestIdentifier () throws Exception {
		Options<String> digestAlgorithm = Options.from(new HashMap<>() {{
			put("digestAlgorithm", "SHA-384");
			put("digestProjectName", "project1");
		}});
		DocumentFactory documentFactory = new DocumentFactory().configure(digestAlgorithm);
		final Extractor extractor = new Extractor(documentFactory).configure(digestAlgorithm);
		TikaDocument tikaDocument1 = extractor.extract(Paths.get(getClass().getResource("/documents/ocr/simple.tiff").getPath()));

		Assert.assertNotNull(tikaDocument1.getId());
	}

	@Test
	public void testDocumentUseCorrectLanguage () throws IOException {
		DocumentFactory documentFactory = new DocumentFactory().configure(Options.from(new HashMap<>() {{
			put("language", "zho");
		}}));
		final Extractor extractor = new Extractor(documentFactory);
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
		TikaDocument tikaDocument = extractor.extract(Paths.get(getClass().getResource("/documents/ocr/embedded.pdf").getPath()));
		String text;

		try (final Reader reader = tikaDocument.getReader()) {
			text = Spewer.toString(reader);
		}

		Assert.assertEquals("application/pdf", tikaDocument.getMetadata().get(Metadata.CONTENT_TYPE));
		Assert.assertThat(text, RegexMatcher.matchesRegex("^\\s+HEAVY\\sMETAL\\s+HEAVY\\sMETAL\\s+$"));
	}

	@Test
	public void testIgnoreEmbeds() throws Throwable {
		extractor.setEmbedHandling(Extractor.EmbedHandling.IGNORE);
		Assert.assertEquals(extractor.getEmbedHandling(), Extractor.EmbedHandling.IGNORE);

		TikaDocument tikaDocument = extractor.extract(Paths.get(getClass().getResource("/documents/ocr/embedded.pdf").getPath()));

		String text;

		try (final Reader reader = tikaDocument.getReader()) {
			text = Spewer.toString(reader);
		}

		Assert.assertEquals("application/pdf", tikaDocument.getMetadata().get(Metadata.CONTENT_TYPE));
		Assert.assertEquals("\n\n\n\n", text);
	}

	@Test
	public void testDisableOcrOnEmbed() throws Throwable {
		extractor.disableOcr();

		TikaDocument tikaDocument = extractor.extract(Paths.get(getClass().getResource("/documents/ocr/embedded.pdf").getPath()));

		String text;

		try (final Reader reader = tikaDocument.getReader()) {
			text = Spewer.toString(reader);
		}

		Assert.assertEquals("application/pdf", tikaDocument.getMetadata().get(Metadata.CONTENT_TYPE));
		Assert.assertEquals("\n\n\n\n", text);
	}

	@Test
	public void testHtmlOutput() throws Throwable {
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
	public void testHtmlOutputWithEmbeddedEmbeds() throws Throwable {
		extractor.setOutputFormat(Extractor.OutputFormat.HTML);
		Assert.assertEquals(extractor.getOutputFormat(), Extractor.OutputFormat.HTML);

		TikaDocument tikaDocument = extractor.extract(Paths.get(getClass().getResource("/documents/ocr/embedded.pdf").getPath()));

		String text;
		try (final Reader reader = tikaDocument.getReader()) {
			text = Spewer.toString(reader);
		}

		Assert.assertEquals("application/pdf", tikaDocument.getMetadata().get(Metadata.CONTENT_TYPE));
		Assert.assertEquals(getExpected("/expected/embedded-data-uri-pdf.html"), text);
	}

	@Test
	public void testRecursiveEmbedded() throws Exception {
		TikaDocument tikaDocument = extractor.extract(Paths.get(getClass().getResource("/documents/recursive_embedded.docx").getPath()));

		/* we have to use a spewer and not just extract and check the structure of the tikaDocument
		 because the principle is to stream the embedded documents to avoid mounting all the documents in memory.
		 That could be an issue for big documents like mailboxes, zips or tarballs.

		 see https://cwiki.apache.org/confluence/display/tika/RecursiveMetadata#Jukka.27s_RecursiveMetadata_Parser
		 "A downside to the wrapper is that it breaks the Tika goal of streaming output"

		 so we just use a print stream spewer and check that all the tree has been parsed */
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		new PrintStreamSpewer(new PrintStream(outputStream), new FieldNames()).write(tikaDocument);
		String allContents = new String(outputStream.toByteArray());

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
	public void testPageExtractionForPdf() throws Exception {
		TikaDocument doc = extractor.extract(Paths.get(getClass().getResource("/documents/ocr/embedded.pdf").getPath()));
		List<Pair<Long, Long>> pageIndices = extractor.extractPageIndices(Paths.get(getClass().getResource("/documents/ocr/embedded.pdf").getPath()));

		String text;
		try (final Reader reader = doc.getReader()) {
			text = Spewer.toString(reader);
		}

		assertThat(pageIndices).isNotNull();
		assertThat(pageIndices).isEqualTo(List.of(Pair.of(0L, 16L), Pair.of(17L,33L)));
		assertThat(text).hasSize(33 + 1);

		String expectedPage = """
		
		HEAVY
		METAL
		
		
		
		""";
		assertThat(getPage(pageIndices.get(0), text)).isEqualTo(expectedPage);
		assertThat(getPage(pageIndices.get(1), text)).isEqualTo(expectedPage);
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
