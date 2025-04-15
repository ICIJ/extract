package org.icij.extract.parser;

import java.io.Serial;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.EmptyParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.WriteOutContentHandler;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.io.Writer;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

public class CachingTesseractOCRParserTest {

	private static final Path tmpDir = Paths.get(System.getProperty("java.io.tmpdir"), "tesseract-cache");

	private final URL simple = getClass().getResource("/documents/ocr/simple.tiff");

	@BeforeClass
	public static void setUp() throws IOException {
		if (!Files.isDirectory(tmpDir)) {
			Files.createDirectory(tmpDir);
		}
	}

	@AfterClass
	public static void cleanUp() throws IOException {
		Files.list(tmpDir).forEach(path -> {try {
			Files.delete(path);
		} catch (final IOException ignored) { }});
	}


	@Test
	public void testWriteToCache() throws Throwable {
		final Path simple = Paths.get(this.simple.toURI());

		Writer writer = new StringWriter();
		final AtomicInteger hit = new AtomicInteger(), miss = new AtomicInteger();

		EmptyParser inner = new EmptyParser();
		final CacheParserDecorator decorated = new CacheParserDecorator(inner, tmpDir) {

			@Serial
			private static final long serialVersionUID = 6551690243986921730L;

			@Override
			public void cacheHit() {
				hit.incrementAndGet();
			}

			@Override
			public void cacheMiss() {
				miss.incrementAndGet();
			}
		};

		try (final InputStream in = Files.newInputStream(simple)) {
			decorated.parse(in, new WriteOutContentHandler(writer), new Metadata(), new ParseContext());
		}

		Assert.assertEquals("HEAVY\nMETAL", writer.toString().trim());
		Assert.assertEquals(0, hit.get());
		Assert.assertEquals(1, miss.get());

		// Try again from the cache.
		writer = new StringWriter();
		try (final InputStream in = Files.newInputStream(simple)) {
			decorated.parse(in, new WriteOutContentHandler(writer), new Metadata(), new ParseContext());
		}

		Assert.assertEquals("HEAVY\nMETAL", writer.toString().trim());
		Assert.assertEquals(1, hit.get());
		Assert.assertEquals(1, miss.get());
	}
}
