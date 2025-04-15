package org.icij.extract.parser;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.ParserDecorator;
import org.apache.tika.sax.TeeContentHandler;
import org.apache.tika.sax.WriteOutContentHandler;
import org.apache.tika.sax.XHTMLContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.*;
import java.nio.file.*;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;

public class CacheParserDecorator extends ParserDecorator {

	@Serial
	private static final long serialVersionUID = -5718575465763633880L;

	private final Path cachePath;
	private final int acquireLockTimeoutS;

	public CacheParserDecorator(Parser parser, final Path cachePath) {
		this(parser, cachePath, 120);
	}

	public CacheParserDecorator(Parser parser, final Path cachePath, int acquireLockTimeoutS) {
		super(parser);
		this.cachePath = cachePath;
		this.acquireLockTimeoutS = acquireLockTimeoutS;
	}

	@Override
	public void parse(InputStream stream, ContentHandler handler, Metadata metadata, ParseContext context) throws IOException, SAXException, TikaException {
		cachedParse(stream, handler, metadata, context, false);
	}

	protected void cacheHit() {
	}

	protected void cacheMiss() {
	}

	private void fallbackParse(final InputStream in, final ContentHandler handler, final Metadata metadata, final ParseContext context, final boolean inline)
			throws SAXException, IOException, TikaException {
		if (inline) {
			final XHTMLContentHandler xhtml;

			if (handler instanceof XHTMLContentHandler) {
				xhtml = (XHTMLContentHandler) handler;
			} else {
				xhtml = new XHTMLContentHandler(handler, metadata);
			}

			super.parse(in, xhtml, metadata, context);
		} else {
			super.parse(in, handler, metadata, context);
		}
	}

	private void parseToCache(final TikaInputStream tis, final ContentHandler handler, final Metadata metadata,
	                          final ParseContext context, final boolean inline,
	                          final Writer writer) throws SAXException, IOException, TikaException {
		final ContentHandler tee = new TeeContentHandler(handler, new WriteOutContentHandler(writer));

		if (inline) {
			super.parse(tis, new XHTMLContentHandler(tee, metadata), metadata, context);
		} else {
			super.parse(tis, tee, metadata, context);
		}
	}

	private void readFromCache(final Reader reader, final ContentHandler handler, final Metadata metadata)
			throws SAXException, IOException {
		final XHTMLContentHandler xhtml;

		if (handler instanceof XHTMLContentHandler) {
			xhtml = (XHTMLContentHandler) handler;
		} else {
			xhtml = new XHTMLContentHandler(handler, metadata);
		}

		xhtml.startElement("div", "class", "ocr");
		readFully(reader, xhtml);
		xhtml.endElement("div");
	}

	private void readFully(final Reader reader, final XHTMLContentHandler xhtml) throws IOException, SAXException {
		final char[] buffer = new char[1024];

		for (int n = reader.read(buffer); n != -1; n = reader.read(buffer)) {
			if (n > 0) {
				xhtml.characters(buffer, 0, n);
			}
		}
	}

	private boolean acquireLock(final Path cacheLock)
			throws IOException, InterruptedException {
		for (int i = 0, l = acquireLockTimeoutS + 1; i < l; i++) {
			try {
				Files.createFile(cacheLock);
				return true;
			} catch (final FileAlreadyExistsException e) {
				TimeUnit.SECONDS.sleep(1);
			}
		}

		return false;
	}

	private void cachedParse(final InputStream in, final ContentHandler handler, final Metadata metadata,
	                         final ParseContext context, final boolean inline)
			throws IOException, SAXException, TikaException {
		try (final TikaInputStream tis = TikaInputStream.get(in)) {
			cachedParse(tis, handler, metadata, context, inline);
		} catch (final InterruptedException e) {
			throw new TikaException("Interrupted.", e);
		}
	}

	private void cachedParse(final TikaInputStream tis, final ContentHandler handler, final Metadata metadata,
	                        final ParseContext context, final boolean inline)
			throws IOException, SAXException, TikaException, InterruptedException {
		final String hash;

		try (final InputStream buffered = Files.newInputStream(tis.getPath())) {
			hash = DigestUtils.sha256Hex(buffered);
		}

		final Path cachePath = this.cachePath.resolve(hash);
		final Path cacheLock = this.cachePath.resolve(hash + ".lock");

		// Acquire a lock both for reading and for writing.
		// If the lock can't be acquired, parse without caching.
		if (!acquireLock(cacheLock)) {
			fallbackParse(tis, handler, metadata, context, inline);
			return;
		}

		// You won't know for sure until you try....
		try (final Reader reader = Files.newBufferedReader(cachePath, UTF_8)) {
			cacheHit();
			readFromCache(reader, handler, metadata);
		} catch (final NoSuchFileException e) {
			final Path cacheTemp = this.cachePath.resolve(hash + ".tmp");

			// Write to a temporary file and only move to the final path if parsing completes successfully.
			// This way we ensure that we don't cache partial results from Tesseract if there's an error.
			try (final Writer writer = Files.newBufferedWriter(cacheTemp, UTF_8, StandardOpenOption.CREATE)) {
				cacheMiss();
				parseToCache(tis, handler, metadata, context, inline, writer);
			}

			Files.move(cacheTemp, cachePath, StandardCopyOption.ATOMIC_MOVE);
		} finally {
			Files.deleteIfExists(cacheLock);
		}
	}
}
