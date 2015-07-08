package org.icij.extract.core;

import org.icij.extract.interval.TimeDuration;

import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Locale;

import java.util.logging.Level;
import java.util.logging.Logger;

import java.util.concurrent.TimeUnit;

import java.nio.file.Path;

import java.io.File;
import java.io.Reader;
import java.io.InputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.tika.config.TikaConfig;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaMetadataKeys;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.EmptyParser;
import org.apache.tika.parser.CompositeParser;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.html.HtmlMapper;
import org.apache.tika.parser.html.DefaultHtmlMapper;
import org.apache.tika.parser.ocr.TesseractOCRConfig;
import org.apache.tika.parser.ocr.TesseractOCRParser;
import org.apache.tika.parser.pdf.PDFParserConfig;
import org.apache.tika.extractor.EmbeddedDocumentExtractor;
import org.apache.tika.mime.MediaType;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.exception.TikaException;

/**
 * A reusable class that sets up Tika parsers based on runtime options.
 *
 * @since 1.0.0-beta
 */
public class Extractor {

	public static enum OutputFormat {
		HTML, TEXT;

		public static OutputFormat parse(String outputFormat) {
			try {
				return valueOf(outputFormat.toUpperCase(Locale.ROOT));
			} catch (IllegalArgumentException e) {
				throw new IllegalArgumentException(String.format("\"%s\" is not a valid output format.", outputFormat));
			}
		}
	}

	public static enum EmbedHandling {
		IGNORE, EXTRACT, EMBED;

		public static EmbedHandling parse(String embedHandling) {
			try {
				return valueOf(embedHandling.toUpperCase(Locale.ROOT));
			} catch (IllegalArgumentException e) {
				throw new IllegalArgumentException(String.format("\"%s\" is not a valid embed handling mode.", embedHandling));
			}
		}
	}

	private final Logger logger;

	private boolean ocrDisabled = false;

	private final TikaConfig config = TikaConfig.getDefaultConfig();
	private final TesseractOCRConfig ocrConfig = new TesseractOCRConfig();
	private final PDFParserConfig pdfConfig = new PDFParserConfig();

	private final Set<MediaType> excludedTypes = new HashSet<MediaType>();

	private EmbedHandling embedHandling = EmbedHandling.EXTRACT;
	private OutputFormat outputFormat = OutputFormat.TEXT;

	public Extractor(Logger logger) {
		this.logger = logger;

		// Run OCR on images contained within PDFs.
		pdfConfig.setExtractInlineImages(true);

		// By default, only the object IDs are used for determining uniqueness.
		// In scanned documents under test from the Panama registry, different embedded images had the same ID, leading to incomplete OCRing when uniqueness detection was turned on.
		pdfConfig.setExtractUniqueInlineImagesOnly(false);
		pdfConfig.setUseNonSequentialParser(true);
	}

	public void setEmbedHandling(EmbedHandling embedHandling) {
		this.embedHandling = embedHandling;
	}

	public EmbedHandling getEmbedHandling() {
		return embedHandling;
	}

	public void setOutputFormat(OutputFormat outputFormat) {
		this.outputFormat = outputFormat;
	}

	public OutputFormat getOutputFormat() {
		return outputFormat;
	}

	public void setOcrLanguage(String ocrLanguage) {
		ocrConfig.setLanguage(ocrLanguage);
	}

	public void setOcrTimeout(int ocrTimeout) {
		ocrConfig.setTimeout(ocrTimeout);
	}

	public void setOcrTimeout(String duration) {
		setOcrTimeout((int) TimeDuration.parseTo(duration, TimeUnit.SECONDS));
	}

	public void setOcrTimeout(TimeDuration duration) {
		setOcrTimeout((int) duration.to(TimeUnit.SECONDS));
	}

	public void disableOcr() {
		if (!ocrDisabled) {
			excludeParser(TesseractOCRParser.class);
			ocrDisabled = true;
			pdfConfig.setExtractInlineImages(false);
		}
	}

	public Reader extract(final Path file) throws IOException, FileNotFoundException, TikaException {
		return extract(file, new Metadata());
	}

	public Reader extract(final Path file, final Metadata metadata) throws IOException, FileNotFoundException, TikaException {

		// Using the #get method like so, Tika will set the resource name and content length
		// automatically on the metadata.
		return extract(TikaInputStream.get(file.toFile(), metadata), metadata);
	}

	public Reader extract(final TikaInputStream input, final Metadata metadata) throws IOException, TikaException {
		final ParseContext context = new ParseContext();
		final AutoDetectParser parser = new AutoDetectParser(config);

		// Attempt to use the more modern Path interface throughout, except
		// where Tika requires File and doesn't support Path.
		final Path file = input.getFile().toPath();

		if (!ocrDisabled) {
			context.set(TesseractOCRConfig.class, ocrConfig);
		}

		context.set(PDFParserConfig.class, pdfConfig);
		parser.setFallback(new ErrorParser(parser, excludedTypes));

		// Only include "safe" tags in the HTML output. This excludes script tags and objects.
		if (OutputFormat.HTML == outputFormat) {
			context.set(HtmlMapper.class, DefaultHtmlMapper.INSTANCE);
		}

		ParsingReader reader;

		// Return a parsing reader that embeds embedded objects as data URIs.
		if (OutputFormat.HTML == outputFormat && EmbedHandling.EMBED == embedHandling) {
			return new EmbeddingHTMLParsingReader(logger, parser, input, metadata, context);
		}

		// For all output types, allow text to be optionally inline-extracted into the main stream.
		if (EmbedHandling.EXTRACT == embedHandling) {
			context.set(Parser.class, parser);
			context.set(EmbeddedDocumentExtractor.class, new ParsingEmbeddedDocumentExtractor(logger, file, context));
		} else {
			context.set(Parser.class, EmptyParser.INSTANCE);
			context.set(EmbeddedDocumentExtractor.class, new DenyingEmbeddedDocumentExtractor());
		}

		if (OutputFormat.TEXT == outputFormat) {
			reader = new TextParsingReader(logger, parser, input, metadata, context);
		} else {
			reader = new HTMLParsingReader(logger, parser, input, metadata, context);
		}

		return reader;
	}

	private void excludeParser(Class exclude) {
		final CompositeParser composite = (CompositeParser) config.getParser();
		final Map<MediaType, Parser> parsers = composite.getParsers();
		final Iterator<Map.Entry<MediaType, Parser>> iterator = parsers.entrySet().iterator();
		final ParseContext context = new ParseContext();

		while (iterator.hasNext()) {
			Map.Entry<MediaType, Parser> pair = iterator.next();
			Parser parser = pair.getValue();

			if (exclude == parser.getClass()) {
				iterator.remove();
				excludedTypes.addAll(parser.getSupportedTypes(context));
			}
		}

		composite.setParsers(parsers);
	}
}
