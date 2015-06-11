package org.icij.extract.core;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import java.util.logging.Level;
import java.util.logging.Logger;

import java.nio.file.Path;

import java.io.File;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.tika.config.TikaConfig;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaMetadataKeys;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.CompositeParser;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParsingReader;
import org.apache.tika.parser.ocr.TesseractOCRConfig;
import org.apache.tika.parser.ocr.TesseractOCRParser;
import org.apache.tika.parser.pdf.PDFParserConfig;
import org.apache.tika.extractor.EmbeddedDocumentExtractor;
import org.apache.tika.mime.MediaType;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.exception.TikaException;

/**
 * Extract
 *
 * @author Matthew Caruana Galizia <mcaruana@icij.org>
 * @version 1.0.0-beta
 * @since 1.0.0-beta
 */
public class Extractor {
	private final Logger logger;

	private boolean ocrDisabled = false;
	private boolean embedsIgnored = false;

	private final TikaConfig config = TikaConfig.getDefaultConfig();
	private final TesseractOCRConfig ocrConfig = new TesseractOCRConfig();
	private final PDFParserConfig pdfConfig = new PDFParserConfig();

	private final List<Parser> excludedParsers = new ArrayList<Parser>();

	public Extractor(Logger logger) {
		this.logger = logger;

		// Run OCR on images contained within PDFs.
		pdfConfig.setExtractInlineImages(true);

		// By default, only the object IDs are used for determining uniqueness.
		// In scanned documents under test from the Panama registry, different embedded images had the same ID, leading to incomplete OCRing when uniqueness detection was turned on.
		pdfConfig.setExtractUniqueInlineImagesOnly(false);
		pdfConfig.setUseNonSequentialParser(true);
	}

	public void setOcrLanguage(String ocrLanguage) {
		ocrConfig.setLanguage(ocrLanguage);
	}

	public void setOcrTimeout(int ocrTimeout) {
		ocrConfig.setTimeout(ocrTimeout);
	}

	public void disableOcr() {
		if (!ocrDisabled) {
			excludeParsers(TesseractOCRParser.class);
			ocrDisabled = true;
			pdfConfig.setExtractInlineImages(false);
		}
	}

	public void ignoreEmbeds() {
		embedsIgnored = true;
	}

	public ParsingReader extract(final Path file) throws IOException, FileNotFoundException, TikaException {
		return extract(new FileInputStream(file.toString()), file, new Metadata());
	}

	public ParsingReader extract(final Path file, final Metadata metadata) throws IOException, FileNotFoundException, TikaException {
		return extract(new FileInputStream(file.toString()), file, metadata);
	}

	public ParsingReader extract(final InputStream stream, final Path file, final Metadata metadata) throws IOException, TikaException {
		final ParseContext context = new ParseContext();
		final AutoDetectParser parser = new AutoDetectParser(config);

		if (!ocrDisabled) {
			context.set(TesseractOCRConfig.class, ocrConfig);
		}

		// Set up recursive parsing of archives and documents with embedded images.
		if (!embedsIgnored) {
			context.set(Parser.class, parser);
			context.set(EmbeddedDocumentExtractor.class, new EmbedExtractor(logger, file, context));
		}

		metadata.set(TikaMetadataKeys.RESOURCE_NAME_KEY, file.getFileName().toString());
		context.set(PDFParserConfig.class, pdfConfig);
		parser.setFallback(new FallbackParser(parser, excludedParsers));

		return new ParsingReader(parser, TikaInputStream.get(stream), metadata, context);
	}

	private void excludeParsers(Class... classes) {
		final CompositeParser composite = (CompositeParser) config.getParser();
		final Map<MediaType, Parser> parsers = composite.getParsers();

		final Iterator<Map.Entry<MediaType, Parser>> iterator = parsers.entrySet().iterator();
		final List<Class> exclude = Arrays.asList(classes);

		while (iterator.hasNext()) {
			Map.Entry<MediaType, Parser> pair = iterator.next();
			Parser parser = pair.getValue();

			if (!exclude.contains(parser.getClass())) {
				continue;
			}

			iterator.remove();
			if (!excludedParsers.contains(parser)) {
				excludedParsers.add(parser);
			}
		}

		composite.setParsers(parsers);
	}
}
