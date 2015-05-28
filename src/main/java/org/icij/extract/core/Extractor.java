package org.icij.extract.core;

import java.util.Map;
import java.util.Set;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import java.util.logging.Level;
import java.util.logging.Logger;

import java.nio.file.Path;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import java.io.File;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.tika.config.TikaConfig;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.HttpHeaders;
import org.apache.tika.metadata.TikaMetadataKeys;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.CompositeParser;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParsingReader;
import org.apache.tika.parser.html.HtmlParser;
import org.apache.tika.parser.ocr.TesseractOCRConfig;
import org.apache.tika.parser.ocr.TesseractOCRParser;
import org.apache.tika.parser.pdf.PDFParserConfig;
import org.apache.tika.extractor.EmbeddedDocumentExtractor;
import org.apache.tika.mime.MediaType;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.exception.TikaException;

import org.xml.sax.ContentHandler;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public class Extractor {
	private final Logger logger;

	private Charset outputEncoding = StandardCharsets.UTF_8;
	private String ocrLanguage = "eng";
	private boolean ocrDisabled = false;

	private final Path file;

	private final TikaConfig config = TikaConfig.getDefaultConfig();
	private final Collection<Class<TesseractOCRParser>> excludedParsers = new ArrayList<Class<TesseractOCRParser>>();

	public Extractor (Logger logger, Path file) {
		this.logger = logger;
		this.file = file;
	}

	public void setOutputEncoding(Charset outputEncoding) {
		this.outputEncoding = outputEncoding;
	}

	public Charset getOutputEncoding() {
		return outputEncoding;
	}

	public void setOcrLanguage(String ocrLanguage) {
		this.ocrLanguage = ocrLanguage;
	}

	public void disableOcr() {
		if (!ocrDisabled) {
			excludedParsers.add(TesseractOCRParser.class);
			ocrDisabled = true;
		}
	}

	public ParsingReader extract(final Path file) throws IOException, FileNotFoundException, TikaException {
		final Metadata metadata = new Metadata();

		final ParseContext context = new ParseContext();
		final PDFParserConfig pdfConfig = new PDFParserConfig();

		excludeParsers();

		final AutoDetectParser parser = new AutoDetectParser(config);

		if (!ocrDisabled) {
			final TesseractOCRConfig ocrConfig = new TesseractOCRConfig();

			ocrConfig.setLanguage(ocrLanguage);
			context.set(TesseractOCRConfig.class, ocrConfig);

			// Run OCR on images contained within PDFs.
			pdfConfig.setExtractInlineImages(true);

			// By default, only the object IDs are used for determining uniqueness.
			// In scanned documents under test from the Panama registry, different embedded images had the same ID, leading to incomplete OCRing when uniqueness detection was turned on.
			pdfConfig.setExtractUniqueInlineImagesOnly(false);
		}

		pdfConfig.setUseNonSequentialParser(true);
		context.set(PDFParserConfig.class, pdfConfig);

		parser.setFallback(new Parser() {
			public Set<MediaType> getSupportedTypes(ParseContext context) {
				return parser.getSupportedTypes(context);
			}

			public void parse(InputStream inputStream, ContentHandler contentHandler, Metadata metadata, ParseContext context) throws TikaException {
				throw new TikaException("Unsupported media type: " + metadata.get(HttpHeaders.CONTENT_TYPE) + ".");
			}
		});

		// Set up recursive parsing of archives and documents with embedded images.
		context.set(Parser.class, parser);
		context.set(EmbeddedDocumentExtractor.class, new EmbedExtractor(logger, file, context));

		metadata.set(TikaMetadataKeys.RESOURCE_NAME_KEY, file.getFileName().toString());

		final TikaInputStream stream = TikaInputStream.get(new FileInputStream(file.toString()));
		final ParsingReader reader = new ParsingReader(parser, stream, metadata, context);

		return reader;
	}

	private void excludeParsers() {
		final CompositeParser compositeParser = (CompositeParser) config.getParser();
		final Map<MediaType, Parser> parsers = compositeParser.getParsers();

		final Iterator iterator = parsers.entrySet().iterator();

		if (excludedParsers.size() == 0) {
			return;
		}

		while (iterator.hasNext()) {
			Map.Entry pair = (Map.Entry) iterator.next();

			if (excludedParsers.contains(pair.getValue().getClass())) {
				iterator.remove();
			}
		}

		compositeParser.setParsers(parsers);
	}
}
