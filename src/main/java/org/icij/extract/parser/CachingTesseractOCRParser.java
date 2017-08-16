package org.icij.extract.parser;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TemporaryResources;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.ocr.TesseractOCRConfig;
import org.apache.tika.parser.ocr.TesseractOCRParser;
import org.apache.tika.sax.TeeContentHandler;
import org.apache.tika.sax.WriteOutContentHandler;
import org.apache.tika.sax.XHTMLContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.nio.charset.StandardCharsets.UTF_8;

public class CachingTesseractOCRParser extends TesseractOCRParser {

	private static final long serialVersionUID = -5718575465763633880L;

	private static final Lock lock = new ReentrantLock();
	private static final Map<Path, Condition> inProgress = new ConcurrentHashMap<>();

	private final Path outputPath;

	public CachingTesseractOCRParser(final Path outputPath) {
		this.outputPath = outputPath;
	}

	@Override
	public void parse(InputStream in, ContentHandler handler, Metadata metadata, ParseContext context)
			throws IOException, SAXException, TikaException {
		if (null != outputPath) {
			cachedParse(in, handler, metadata, context, null, false);
		} else {
			super.parse(in, handler, metadata, context);
		}
	}

	@Override
	public void parseInline(InputStream in, XHTMLContentHandler xhtml, ParseContext context, TesseractOCRConfig config)
			throws IOException, SAXException, TikaException {
		if (null != outputPath) {
			cachedParse(in, xhtml, new Metadata(), context, config, true);
		} else {
			super.parseInline(in, xhtml, context, config);
		}
	}

	public void cacheHit() {
	}

	public void cacheMiss() {
	}

	private void cachedParse(final InputStream in, final ContentHandler handler, final Metadata metadata,
	                        final ParseContext context, final TesseractOCRConfig config, final boolean inline)
			throws IOException, SAXException,	TikaException {
		try (final TemporaryResources tmp = new TemporaryResources();
		     final TikaInputStream tis = TikaInputStream.get(in, tmp);
		     final InputStream buffered = Files.newInputStream(tis.getPath())) {

			final String hash = DigestUtils.sha256Hex(buffered);
			final Path cachePath = outputPath.resolve("tesseract-result-cache-" + hash);

			if (Files.exists(cachePath)) {
				lock.lock();
				
				try {
					final Condition condition = inProgress.get(cachePath);

					if (null != condition) {
						condition.awaitUninterruptibly();
					}
				} finally {
					lock.unlock();
				}

				cacheHit();

				final XHTMLContentHandler xhtml;

				if (handler instanceof XHTMLContentHandler) {
					xhtml = (XHTMLContentHandler) handler;
				} else {
					xhtml = new XHTMLContentHandler(handler, metadata);
				}

				xhtml.startElement("div", "class", "ocr");

				try (final Reader reader = Files.newBufferedReader(cachePath, UTF_8)) {
					char[] buffer = new char[1024];

					for (int n = reader.read(buffer); n != -1; n = reader.read(buffer)) {
						if (n > 0) {
							xhtml.characters(buffer, 0, n);
						}
					}
				}

				xhtml.endElement("div");
			} else {
				final Condition condition = lock.newCondition();

				inProgress.putIfAbsent(cachePath, condition);

				cacheMiss();
				try (final Writer writer = Files.newBufferedWriter(cachePath, UTF_8)) {
					final ContentHandler tee = new TeeContentHandler(handler, new WriteOutContentHandler(writer));

					if (inline) {
						super.parseInline(tis, new XHTMLContentHandler(tee, metadata), context, config);
					} else {
						super.parse(tis, tee, metadata, context);
					}
				} finally {
					lock.lock();
					inProgress.remove(cachePath, condition);
					condition.signalAll();
					lock.unlock();
				}
			}
		}
	}
}
