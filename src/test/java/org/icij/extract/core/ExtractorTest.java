package org.icij.extract.core;

import org.icij.extract.document.Document;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.PathIdentifier;
import org.icij.extract.extractor.Extractor;
import org.icij.extract.parser.ExcludedMediaTypeException;
import org.icij.extract.test.*;

import java.io.Reader;
import java.io.InputStream;
import java.io.IOException;
import java.nio.file.NoSuchFileException;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.charset.StandardCharsets;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.exception.EncryptedDocumentException;
import org.apache.tika.exception.TikaException;

import org.apache.commons.io.IOUtils;

import org.junit.Test;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

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
			text = IOUtils.toString(reader);
		}

		Assert.assertEquals("image/tiff", document.getMetadata().get(Metadata.CONTENT_TYPE));
		Assert.assertEquals("HEAVY\nMETAL\n\n\n", text);
	}

	@Test
	public void testDisableOcr() throws Throwable {
		final Extractor extractor = new Extractor();
		extractor.disableOcr();

		final Document document = factory.create(getClass().getResource("/documents/ocr/simple.tiff"));

		thrown.expect(IOException.class);
		thrown.expectMessage("");
		thrown.expectCause(new CauseMatcher(ExcludedMediaTypeException.class, "Excluded media type: image/tiff"));

		final int read;

		try (Reader reader = extractor.extract(document)) {
			read = reader.read();
		} catch (IOException e) {
			Assert.assertEquals("image/tiff", document.getMetadata().get(Metadata.CONTENT_TYPE));
			throw e;
		}

		Assert.fail(String.format("Read \"%d\" while expecting exception.", read));
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

		try (Reader reader = extractor.extract(document)) {
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
		thrown.expectCause(new CauseMatcher(TikaException.class, "Unsupported media type: application/octet-stream"));

		final int read;

		try (Reader reader = extractor.extract(document)) {
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

		try (Reader reader = extractor.extract(document)) {
			text = IOUtils.toString(reader);
		}

		Assert.assertEquals("application/pdf", document.getMetadata().get(Metadata.CONTENT_TYPE));
		Assert.assertEquals("\nHEAVY\nMETAL\n\n\n\n\n\nHEAVY\nMETAL\n\n\n\n\n", text);
	}

	@Test
	public void testIgnoreEmbeds() throws Throwable {
		final Extractor extractor = new Extractor();

		extractor.setEmbedHandling(Extractor.EmbedHandling.IGNORE);
		Assert.assertEquals(extractor.getEmbedHandling(), Extractor.EmbedHandling.IGNORE);

		final Document document = factory.create(getClass().getResource("/documents/ocr/embedded.pdf"));

		String text;

		try (Reader reader = extractor.extract(document)) {
			text = IOUtils.toString(reader);
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

		try (Reader reader = extractor.extract(document)) {
			text = IOUtils.toString(reader);
		}

		Assert.assertEquals("application/pdf", document.getMetadata().get(Metadata.CONTENT_TYPE));
		Assert.assertEquals("\n\n\n\n", text);
	}

	@Test
	public void testHtmlOutput() throws Throwable {
		final Extractor extractor = new Extractor();
		extractor.setOutputFormat(Extractor.OutputFormat.HTML);

		final Document document = factory.create(getClass().getResource("/documents/text/utf16.txt"));

		String text;

		try (Reader reader = extractor.extract(document)) {
			text = IOUtils.toString(reader);
		}

		Assert.assertEquals("text/plain; charset=UTF-16LE", document.getMetadata().get(Metadata.CONTENT_TYPE));
		Assert.assertEquals(getExpected("/expected/text/utf16-txt.html"), text);
	}

	@Test
	public void testHtmlOutputWithEmbeds() throws Throwable {
		final Extractor extractor = new Extractor();
		extractor.setOutputFormat(Extractor.OutputFormat.HTML);

		final Document document = factory.create(getClass().getResource("/documents/ocr/embedded.pdf"));

		String text;

		try (Reader reader = extractor.extract(document)) {
			text = IOUtils.toString(reader);
		}

		Assert.assertEquals("application/pdf", document.getMetadata().get(Metadata.CONTENT_TYPE));
		Assert.assertEquals(getExpected("/expected/text/embedded-pdf.html"), text);
	}

	@Test
	public void testHtmlOutputWithEmbeddedEmbeds() throws Throwable {
		final Extractor extractor = new Extractor();

		extractor.setOutputFormat(Extractor.OutputFormat.HTML);
		Assert.assertEquals(extractor.getOutputFormat(), Extractor.OutputFormat.HTML);

		extractor.setEmbedHandling(Extractor.EmbedHandling.EMBED);
		Assert.assertEquals(extractor.getEmbedHandling(), Extractor.EmbedHandling.EMBED);

		final Document document = factory.create(getClass().getResource("/documents/ocr/embedded.pdf"));

		String text;

		try (Reader reader = extractor.extract(document)) {
			text = IOUtils.toString(reader);
		}

		Assert.assertEquals("application/pdf", document.getMetadata().get(Metadata.CONTENT_TYPE));
		Assert.assertEquals(getExpected("/expected/text/embedded-data-uri-pdf.html"), text);
	}

	@Test
	public void testFailsWithoutWorkingDirectory() throws Throwable {
		final Extractor extractor = new Extractor();
		final Document document = factory.create(Paths.get("text/plain.txt"));

		Assert.assertNull(extractor.getWorkingDirectory());
		thrown.expect(NoSuchFileException.class);
		thrown.expectMessage("text/plain.txt");

		extractor.extract(document);
	}

	@Test
	public void testSucceedsWithWorkingDirectory() throws Throwable {
		final Extractor extractor = new Extractor();
		final Path workingDirectory = Paths.get(getClass().getResource("/documents").toURI());
		extractor.setWorkingDirectory(workingDirectory);
		Assert.assertEquals(workingDirectory.toString(), extractor.getWorkingDirectory().toString());

		String text;
		try (Reader reader = extractor.extract(factory.create(Paths.get("text/plain.txt")))) {
			text = IOUtils.toString(reader);
		}

		Assert.assertEquals("This is a test.\n\n", text);
	}

	private String getExpected(final String file) throws IOException {
		try (InputStream input = getClass().getResourceAsStream(file)) {
			return IOUtils.toString(input, StandardCharsets.UTF_8);
		}
	}
}
