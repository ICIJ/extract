package org.icij.extract.extractor;

import org.apache.tika.exception.EncryptedDocumentException;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.icij.extract.document.Document;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.PathIdentifier;
import org.icij.spewer.Spewer;
import org.icij.test.CauseMatcher;
import org.icij.test.RegexMatcher;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;

@FixMethodOrder
public class ExtractorTest {

	private final DocumentFactory factory = new DocumentFactory().withIdentifier(new PathIdentifier());

	@Rule
	public final ExpectedException thrown = ExpectedException.none();

	@Test
	public void testOcr() throws Throwable {
		final Extractor extractor = new Extractor();
		final Document document = factory.create(getClass().getResource("/documents/ocr/simple.tiff"));

		String text;

		try (Reader reader = extractor.extract(document)) {
			text = Spewer.toString(reader);
		}

		Assert.assertEquals("image/tiff", document.getMetadata().get(Metadata.CONTENT_TYPE));
		Assert.assertEquals("HEAVY\nMETAL", text.trim());
	}

	@Test
	public void testDisableOcr() throws Throwable {
		final Extractor extractor = new Extractor();
		extractor.disableOcr();

		final Document document = factory.create(getClass().getResource("/documents/ocr/simple.tiff"));
		final Reader reader = extractor.extract(document);

		final int read = reader.read();

		Assert.assertEquals("image/tiff", document.getMetadata().get(Metadata.CONTENT_TYPE));
		Assert.assertEquals(-1, read);
	}

	@Test
	public void testFileNotFound() throws Throwable {
		final Extractor extractor = new Extractor();
		final Document document = factory.create(Paths.get("nothing"));

		thrown.expect(NoSuchFileException.class);
		thrown.expectMessage("nothing");

		extractor.extract(document);
	}

	@Test
	public void testEncryptedPdf() throws Throwable {
		final Extractor extractor = new Extractor();

		final Document document = factory.create(getClass().getResource("/documents/pdf/encrypted.pdf"));

		thrown.expect(IOException.class);
		thrown.expectMessage("");
		thrown.expectCause(new CauseMatcher(EncryptedDocumentException.class, "Unable to process: document is encrypted"));

		final int read;

		try (final Reader reader = extractor.extract(document)) {
			read = reader.read();
		} catch (IOException e) {
			Assert.assertEquals("application/pdf", document.getMetadata().get(Metadata.CONTENT_TYPE));
			throw e;
		}

		Assert.fail(String.format("Read \"%d\" while expecting exception.", read));
	}

	@Test
	public void testGarbage() throws Throwable {
		final Extractor extractor = new Extractor();

		final Document document = factory.create(getClass().getResource("/documents/garbage.bin"));

		thrown.expect(IOException.class);
		thrown.expectMessage("");
		thrown.expectCause(new CauseMatcher(TikaException.class, "Parse error"));

		final int read;

		try (final Reader reader = extractor.extract(document)) {
			read = reader.read();
		} catch (IOException e) {
			Assert.assertEquals("application/octet-stream", document.getMetadata().get(Metadata.CONTENT_TYPE));
			throw e;
		}

		Assert.fail(String.format("Read \"%d\" while expecting exception.", read));
	}

	@Test
	public void testEmbeds() throws Throwable {
		final Extractor extractor = new Extractor();

		final Document document = factory.create(getClass().getResource("/documents/ocr/embedded.pdf"));
		String text;

		try (final Reader reader = extractor.extract(document)) {
			text = Spewer.toString(reader);
		}

		Assert.assertEquals("application/pdf", document.getMetadata().get(Metadata.CONTENT_TYPE));
		//Assert.assertEquals("HEAVY\nMETAL\n\n\n\n\n\nHEAVY\nMETAL", text.trim());
		Assert.assertThat(text, RegexMatcher.matchesRegex("^\\s+HEAVY\\sMETAL\\s+HEAVY\\sMETAL\\s+$"));
	}

	@Test
	public void testIgnoreEmbeds() throws Throwable {
		final Extractor extractor = new Extractor();

		extractor.setEmbedHandling(Extractor.EmbedHandling.IGNORE);
		Assert.assertEquals(extractor.getEmbedHandling(), Extractor.EmbedHandling.IGNORE);

		final Document document = factory.create(getClass().getResource("/documents/ocr/embedded.pdf"));

		String text;

		try (final Reader reader = extractor.extract(document)) {
			text = Spewer.toString(reader);
		}

		Assert.assertEquals("application/pdf", document.getMetadata().get(Metadata.CONTENT_TYPE));
		Assert.assertEquals("\n\n\n\n", text);
	}

	@Test
	public void testDisableOcrOnEmbed() throws Throwable {
		final Extractor extractor = new Extractor();
		extractor.disableOcr();

		final Document document = factory.create(getClass().getResource("/documents/ocr/embedded.pdf"));

		String text;

		try (final Reader reader = extractor.extract(document)) {
			text = Spewer.toString(reader);
		}

		Assert.assertEquals("application/pdf", document.getMetadata().get(Metadata.CONTENT_TYPE));
		Assert.assertEquals("\n\n\n\n", text);
	}

	@Test
	@Ignore
	public void testHtmlOutput() throws Throwable {
		final Extractor extractor = new Extractor();
		extractor.setOutputFormat(Extractor.OutputFormat.HTML);

		final Document document = factory.create(getClass().getResource("/documents/text/utf16.txt"));

		String text;

		try (final Reader reader = extractor.extract(document)) {
			text = Spewer.toString(reader);
		}

		Assert.assertEquals("text/plain; charset=UTF-16LE", document.getMetadata().get(Metadata.CONTENT_TYPE));
		Assert.assertEquals(getExpected("/expected/utf16-txt.html"), text);
	}

	@Test
	@Ignore
	public void testHtmlOutputWithEmbeds() throws Throwable {
		final Extractor extractor = new Extractor();
		extractor.setOutputFormat(Extractor.OutputFormat.HTML);

		final Document document = factory.create(getClass().getResource("/documents/ocr/embedded.pdf"));

		String text;

		try (final Reader reader = extractor.extract(document)) {
			text = Spewer.toString(reader);
		}

		Assert.assertEquals("application/pdf", document.getMetadata().get(Metadata.CONTENT_TYPE));
		Assert.assertEquals(getExpected("/expected/embedded-pdf.html"), text);
	}

	@Test
	@Ignore
	public void testHtmlOutputWithEmbeddedEmbeds() throws Throwable {
		final Extractor extractor = new Extractor();

		extractor.setOutputFormat(Extractor.OutputFormat.HTML);
		Assert.assertEquals(extractor.getOutputFormat(), Extractor.OutputFormat.HTML);

		//extractor.setEmbedHandling(Extractor.EmbedHandling.EMBED);
		//Assert.assertEquals(extractor.getEmbedHandling(), Extractor.EmbedHandling.EMBED);

		final Document document = factory.create(getClass().getResource("/documents/ocr/embedded.pdf"));

		String text;

		try (final Reader reader = extractor.extract(document)) {
			text = Spewer.toString(reader);
		}

		Assert.assertEquals("application/pdf", document.getMetadata().get(Metadata.CONTENT_TYPE));
		Assert.assertEquals(getExpected("/expected/embedded-data-uri-pdf.html"), text);
	}

	private String getExpected(final String file) throws IOException {
		try (final Reader input = new InputStreamReader(getClass().getResourceAsStream(file), StandardCharsets.UTF_8)) {
			return Spewer.toString(input);
		}
	}
}
