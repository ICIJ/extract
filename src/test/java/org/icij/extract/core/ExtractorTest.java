package org.icij.extract.core;

import java.util.logging.Logger;

import java.io.Reader;
import java.io.InputStream;
import java.io.IOException;
import java.io.FileNotFoundException;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.charset.Charset;
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

	private final Logger logger = Logger.getLogger("extract-test");

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testOcr() throws Throwable {
		final Extractor extractor = new Extractor(logger);
		final Path file = Paths.get(getClass().getResource("/documents/ocr/simple.tiff").toURI());
		final Metadata metadata = new Metadata();
		final Reader reader = extractor.extract(file, metadata);

		String text = null;

		try {
			text = IOUtils.toString(reader);
		} catch (IOException e) {
			throw e;
		} finally {
			reader.close();
		}

		Assert.assertEquals("image/tiff", metadata.get(Metadata.CONTENT_TYPE));
		Assert.assertEquals("HEAVY\nMETAL\n\n\n", text);
	}

	@Test
	public void testDisableOcr() throws Throwable {
		final Extractor extractor = new Extractor(logger);
		extractor.disableOcr();

		final Path file = Paths.get(getClass().getResource("/documents/ocr/simple.tiff").toURI());
		final Metadata metadata = new Metadata();
		final Reader reader = extractor.extract(file, metadata);

		thrown.expect(IOException.class);
		thrown.expectMessage("");
		thrown.expectCause(new CauseMatcher(ExcludedMediaTypeException.class, "Excluded media type: image/tiff"));

		try {
			reader.read();
		} catch (IOException e) {
			Assert.assertEquals("image/tiff", metadata.get(Metadata.CONTENT_TYPE));
			throw e;
		} finally {
			reader.close();
		}
	}

	@Test
	public void testFileNotFound() throws Throwable {
		final Extractor extractor = new Extractor(logger);

		final Path file = Paths.get("nothing");

		thrown.expect(FileNotFoundException.class);
		thrown.expectMessage("nothing (No such file or directory)");

		extractor.extract(file);
	}

	@Test
	public void testEncryptedPdf() throws Throwable {
		final Extractor extractor = new Extractor(logger);

		final Path file = Paths.get(getClass().getResource("/documents/pdf/encrypted.pdf").toURI());
		final Metadata metadata = new Metadata();
		final Reader reader = extractor.extract(file, metadata);

		thrown.expect(IOException.class);
		thrown.expectMessage("");
		thrown.expectCause(new CauseMatcher(EncryptedDocumentException.class, "Unable to process: document is encrypted"));

		try {
			reader.read();
		} catch (IOException e) {
			Assert.assertEquals("application/pdf", metadata.get(Metadata.CONTENT_TYPE));
			throw e;
		} finally {
			reader.close();
		}
	}

	@Test
	public void testGarbage() throws Throwable {
		final Extractor extractor = new Extractor(logger);

		final Path file = Paths.get(getClass().getResource("/documents/garbage.bin").toURI());
		final Metadata metadata = new Metadata();
		final Reader reader = extractor.extract(file, metadata);

		thrown.expect(IOException.class);
		thrown.expectMessage("");
		thrown.expectCause(new CauseMatcher(TikaException.class, "Unsupported media type: application/octet-stream"));

		try {
			reader.read();
		} catch (IOException e) {
			Assert.assertEquals("application/octet-stream", metadata.get(Metadata.CONTENT_TYPE));
			throw e;
		} finally {
			reader.close();
		}
	}

	@Test
	public void testEmbeds() throws Throwable {
		final Extractor extractor = new Extractor(logger);

		final Path file = Paths.get(getClass().getResource("/documents/ocr/embedded.pdf").toURI());
		final Metadata metadata = new Metadata();
		final Reader reader = extractor.extract(file, metadata);

		String text = null;

		try {
			text = IOUtils.toString(reader);
		} catch (IOException e) {
			throw e;
		} finally {
			reader.close();
		}

		Assert.assertEquals("application/pdf", metadata.get(Metadata.CONTENT_TYPE));
		Assert.assertEquals("\nHEAVY\nMETAL\n\n\n\n\n\nHEAVY\nMETAL\n\n\n\n\n", text);
	}

	@Test
	public void testIgnoreEmbeds() throws Throwable {
		final Extractor extractor = new Extractor(logger);
		extractor.setEmbedHandling(Extractor.EmbedHandling.IGNORE);

		final Path file = Paths.get(getClass().getResource("/documents/ocr/embedded.pdf").toURI());
		final Metadata metadata = new Metadata();
		final Reader reader = extractor.extract(file, metadata);

		String text = null;

		try {
			text = IOUtils.toString(reader);
		} catch (IOException e) {
			throw e;
		} finally {
			reader.close();
		}

		Assert.assertEquals("application/pdf", metadata.get(Metadata.CONTENT_TYPE));
		Assert.assertEquals("\n\n\n\n", text);
	}

	@Test
	public void testDisableOcrOnEmbed() throws Throwable {
		final Extractor extractor = new Extractor(logger);
		extractor.disableOcr();

		final Path file = Paths.get(getClass().getResource("/documents/ocr/embedded.pdf").toURI());
		final Metadata metadata = new Metadata();
		final Reader reader = extractor.extract(file, metadata);

		String text = null;

		try {
			text = IOUtils.toString(reader);
		} catch (IOException e) {
			throw e;
		} finally {
			reader.close();
		}

		Assert.assertEquals("application/pdf", metadata.get(Metadata.CONTENT_TYPE));
		Assert.assertEquals("\n\n\n\n", text);
	}

	@Test
	public void testHtmlOutput() throws Throwable {
		final Extractor extractor = new Extractor(logger);
		extractor.setOutputFormat(Extractor.OutputFormat.HTML);

		final Path file = Paths.get(getClass().getResource("/documents/text/utf16.txt").toURI());
		final Metadata metadata = new Metadata();
		final Reader reader = extractor.extract(file, metadata);

		String text = null;

		try {
			text = IOUtils.toString(reader);
		} catch (IOException e) {
			throw e;
		} finally {
			reader.close();
		}

		Assert.assertEquals("text/plain; charset=UTF-16LE", metadata.get(Metadata.CONTENT_TYPE));
		Assert.assertEquals(getExpected("/expected/text/utf16-txt.html"), text);
	}

	@Test
	public void testHtmlOutputWithEmbeds() throws Throwable {
		final Extractor extractor = new Extractor(logger);
		extractor.setOutputFormat(Extractor.OutputFormat.HTML);

		final Path file = Paths.get(getClass().getResource("/documents/ocr/embedded.pdf").toURI());
		final Metadata metadata = new Metadata();
		final Reader reader = extractor.extract(file, metadata);

		String text = null;

		try {
			text = IOUtils.toString(reader);
		} catch (IOException e) {
			throw e;
		} finally {
			reader.close();
		}

		Assert.assertEquals("application/pdf", metadata.get(Metadata.CONTENT_TYPE));
		Assert.assertEquals(getExpected("/expected/text/embedded-pdf.html"), text);
	}

	@Test
	public void testHtmlOutputWithEmbeddedEmbeds() throws Throwable {
		final Extractor extractor = new Extractor(logger);
		extractor.setOutputFormat(Extractor.OutputFormat.HTML);
		extractor.setEmbedHandling(Extractor.EmbedHandling.EMBED);

		final Path file = Paths.get(getClass().getResource("/documents/ocr/embedded.pdf").toURI());
		final Metadata metadata = new Metadata();
		final Reader reader = extractor.extract(file, metadata);

		String text = null;

		try {
			text = IOUtils.toString(reader);
IOUtils.write(text, new java.io.FileWriter("/Users/matt/Downloads/test.html"));
		} catch (IOException e) {
			throw e;
		} finally {
			reader.close();
		}

		Assert.assertEquals("application/pdf", metadata.get(Metadata.CONTENT_TYPE));
		Assert.assertEquals(getExpected("/expected/text/embedded-datauri-pdf.html"), text);
	}

	private String getExpected(final String file) throws IOException {
		return getExpected(file, StandardCharsets.UTF_8);
	}

	private String getExpected(final String file, final Charset encoding) throws IOException {
		final InputStream input = getClass().getResourceAsStream(file.toString());

		try {
			return IOUtils.toString(input, encoding);
		} catch (IOException e) {
			throw e;
		} finally {
			input.close();
		}
	}
}
