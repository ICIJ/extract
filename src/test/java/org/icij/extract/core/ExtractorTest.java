package org.icij.extract.core;

import java.util.logging.Logger;

import java.io.InputStream;
import java.io.IOException;
import java.io.FileNotFoundException;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.exception.EncryptedDocumentException;
import org.apache.tika.exception.TikaException;

import org.apache.commons.io.IOUtils;

import org.junit.Test;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

public class ExtractorTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testOcr() throws Throwable {
		final Logger logger = Logger.getLogger("extract-test");
		final Extractor extractor = new Extractor(logger);

		final Path file = Paths.get(getClass().getResource("/documents/ocr/simple.tiff").toURI());
		final Metadata metadata = new Metadata();
		final ParsingReader reader = extractor.extract(file, metadata);

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
		final Logger logger = Logger.getLogger("extract-test");
		final Extractor extractor = new Extractor(logger);
		extractor.disableOcr();

		final Path file = Paths.get(getClass().getResource("/documents/ocr/simple.tiff").toURI());
		final Metadata metadata = new Metadata();
		final ParsingReader reader = extractor.extract(file, metadata);

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
		final Logger logger = Logger.getLogger("extract-test");
		final Extractor extractor = new Extractor(logger);

		final Path file = Paths.get("nothing");

		thrown.expect(FileNotFoundException.class);
		thrown.expectMessage("nothing (No such file or directory)");

		extractor.extract(file);
	}

	@Test
	public void testEncryptedPdf() throws Throwable {
		final Logger logger = Logger.getLogger("extract-test");
		final Extractor extractor = new Extractor(logger);

		final Path file = Paths.get(getClass().getResource("/documents/pdf/encrypted.pdf").toURI());
		final Metadata metadata = new Metadata();
		final ParsingReader reader = extractor.extract(file, metadata);

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
		final Logger logger = Logger.getLogger("extract-test");
		final Extractor extractor = new Extractor(logger);

		final Path file = Paths.get(getClass().getResource("/documents/garbage.bin").toURI());
		final Metadata metadata = new Metadata();
		final ParsingReader reader = extractor.extract(file, metadata);

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
		final Logger logger = Logger.getLogger("extract-test");
		final Extractor extractor = new Extractor(logger);

		final Path file = Paths.get(getClass().getResource("/documents/ocr/embedded.pdf").toURI());
		final Metadata metadata = new Metadata();
		final ParsingReader reader = extractor.extract(file, metadata);

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
		final Logger logger = Logger.getLogger("extract-test");
		final Extractor extractor = new Extractor(logger);
		extractor.ignoreEmbeds();

		final Path file = Paths.get(getClass().getResource("/documents/ocr/embedded.pdf").toURI());
		final Metadata metadata = new Metadata();
		final ParsingReader reader = extractor.extract(file, metadata);

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
		final Logger logger = Logger.getLogger("extract-test");
		final Extractor extractor = new Extractor(logger);
		extractor.disableOcr();

		final Path file = Paths.get(getClass().getResource("/documents/ocr/embedded.pdf").toURI());
		final Metadata metadata = new Metadata();
		final ParsingReader reader = extractor.extract(file, metadata);

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
}
