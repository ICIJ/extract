package org.icij.extract.core;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;

import java.nio.file.Path;

import java.io.IOException;

import org.apache.tika.config.TikaConfig;
import org.apache.tika.metadata.Metadata;
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

/**
 * A reusable class that sets up Tika parsers based on runtime options.
 *
 * @since 1.0.0-beta
 */
public class Extractor {

	public enum OutputFormat {
		HTML, TEXT;

		public static OutputFormat parse(String outputFormat) {
			try {
				return valueOf(outputFormat.toUpperCase(Locale.ROOT));
			} catch (IllegalArgumentException e) {
				throw new IllegalArgumentException(String.format("\"%s\" is not a valid output format.", outputFormat));
			}
		}
	}

	public enum EmbedHandling {
		IGNORE, EXTRACT, EMBED;

		public static EmbedHandling parse(String embedHandling) {
			try {
				return valueOf(embedHandling.toUpperCase(Locale.ROOT));
			} catch (IllegalArgumentException e) {
				throw new IllegalArgumentException(String.format("\"%s\" is not a valid embed handling mode.", embedHandling));
			}
		}
	}

	public static final Duration DEFAULT_OCR_TIMEOUT = Duration.ofHours(12);

	private boolean ocrDisabled = false;
	private Path workingDirectory = null;

	private final TikaConfig config = TikaConfig.getDefaultConfig();
	private final TesseractOCRConfig ocrConfig = new TesseractOCRConfig();
	private final PDFParserConfig pdfConfig = new PDFParserConfig();

	private final Set<MediaType> excludedTypes = new HashSet<>();

	private EmbedHandling embedHandling = EmbedHandling.EXTRACT;
	private OutputFormat outputFormat = OutputFormat.TEXT;

	/**
	 * Create a new extractor, which will OCR images by default if Tesseract is available locally, extract inline
	 * images from PDF files and OCR them and use PDFBox's non-sequential PDF parser.
	 */
	public Extractor() {

		// Run OCR on images contained within PDFs.
		pdfConfig.setExtractInlineImages(true);

		// By default, only the object IDs are used for determining uniqueness.
		// In scanned documents under test from the Panama registry, different embedded images had the same ID, leading to incomplete OCRing when uniqueness detection was turned on.
		pdfConfig.setExtractUniqueInlineImagesOnly(false);

		// Set a long OCR timeout by default, because Tika's is too short.
		ocrConfig.setTimeout(Math.toIntExact(DEFAULT_OCR_TIMEOUT.getSeconds()));
		ocrConfig.setEnableImageProcessing(0); // See TIKA-2167. Image processing causes OCR to fail.
	}

	/**
	 * Set the embed handling mode.
	 *
	 * @param embedHandling the embed handling mode
	 */
	public void setEmbedHandling(final EmbedHandling embedHandling) {
		this.embedHandling = embedHandling;
	}

	/**
	 * Get the embed handling mode.
	 *
	 * @return the embed handling mode
	 */
	public EmbedHandling getEmbedHandling() {
		return embedHandling;
	}

	/**
	 * Set the output format.
	 *
	 * @param outputFormat the output format
	 */
	public void setOutputFormat(final OutputFormat outputFormat) {
		this.outputFormat = outputFormat;
	}

	/**
	 * Get the extraction output format.
	 *
	 * @return the output format
	 */
	public OutputFormat getOutputFormat() {
		return outputFormat;
	}

	/**
	 * Set the languages used by Tesseract.
	 *
	 * @param ocrLanguage the languages to use, for example "eng" or "ita+spa"
	 */
	public void setOcrLanguage(final String ocrLanguage) {
		ocrConfig.setLanguage(ocrLanguage);
	}

	/**
	 * Instructs Tesseract to attempt OCR for no longer than the given duration in seconds.
	 *
	 * @param ocrTimeout the duration in seconds
	 */
	private void setOcrTimeout(final int ocrTimeout) {
		ocrConfig.setTimeout(ocrTimeout);
	}

	/**
	 * Instructs Tesseract to attempt OCR for no longer than the given duration.
	 *
	 * @param duration the duration before timeout
	 */
	public void setOcrTimeout(final Duration duration) {
		setOcrTimeout(Math.toIntExact(duration.getSeconds()));
	}

	/**
	 * Disable OCR. This method only has an effect if Tesseract is installed.
	 */
	public void disableOcr() {
		if (!ocrDisabled) {
			excludeParser(TesseractOCRParser.class);
			ocrDisabled = true;
			pdfConfig.setExtractInlineImages(false);
		}
	}

	/**
	 * Set the working directory for the extractor when the paths passed to it are relative. All paths passed to
	 * the extraction methods will be resolved from the working directory.
	 *
	 * @param workingDirectory the working directory
	 */
	public void setWorkingDirectory(final Path workingDirectory) {
		this.workingDirectory = workingDirectory;
	}

	/**
	 * Get the the working directory.
	 *
	 * @return the working directory
	 */
	public Path getWorkingDirectory() {
		return workingDirectory;
	}

	/**
	 * A convenience method for when access to metadata is not required.
	 *
	 * @param file the file to extract from
	 */
	public ParsingReader extract(final Path file) throws IOException {
		return extract(file, new Metadata());
	}

	/**
	 * This method will wrap the given {@link Path} in a {@link TikaInputStream} and return a {@link ParsingReader}
	 * which can be used to initiate extraction on demand.
	 *
	 * Internally, this method uses {@link TikaInputStream#get} which ensures that the resource name and content
	 * length metadata properties are set automatically.
	 *
	 * @param file the file to extract from
	 * @param metadata will be populated with metadata extracted from the file
	 */
	public ParsingReader extract(Path file, final Metadata metadata) throws IOException {
		if (null != workingDirectory) {
			file = workingDirectory.resolve(file);
		}

		// Use the the TikaInputStream.get method that accepts a file, because this sets metadata properties like the
		// resource name and size.
		return extract(file, metadata, TikaInputStream.get(file, metadata));
	}

	/**
	 * Extract from the given {@link TikaInputStream}, populating the given metadata object.
	 *
	 * @param input the stream to extract from
	 * @param file the path to the file that is being extracted from
	 * @param metadata the metadata object to populate
	 */
	protected ParsingReader extract(final Path file, final Metadata metadata, final TikaInputStream input) throws
			IOException {
		final ParseContext context = new ParseContext();
		final AutoDetectParser parser = new AutoDetectParser(config);

		if (!ocrDisabled) {
			context.set(TesseractOCRConfig.class, ocrConfig);
		}

		context.set(PDFParserConfig.class, pdfConfig);
		parser.setFallback(new ErrorParser(parser, excludedTypes));

		// Only include "safe" tags in the HTML output from Tika's HTML parser.
		// This excludes script tags and objects.
		if (OutputFormat.HTML == outputFormat) {
			context.set(HtmlMapper.class, DefaultHtmlMapper.INSTANCE);
		}

		ParsingReader reader;

		// Return a parsing reader that embeds embedded objects as data URIs.
		if (OutputFormat.HTML == outputFormat && EmbedHandling.EMBED == embedHandling) {
			return new EmbeddingHTMLParsingReader(parser, input, metadata, context);
		}

		// For all output types, allow text to be optionally inline-extracted into the main stream.
		if (EmbedHandling.EXTRACT == embedHandling) {
			context.set(Parser.class, parser);
			context.set(EmbeddedDocumentExtractor.class, new ParsingEmbeddedDocumentExtractor(file, context));
		} else {
			context.set(Parser.class, EmptyParser.INSTANCE);
			context.set(EmbeddedDocumentExtractor.class, new DenyingEmbeddedDocumentExtractor());
		}

		if (OutputFormat.TEXT == outputFormat) {
			reader = new TextParsingReader(parser, input, metadata, context);
		} else {
			reader = new HTMLParsingReader(parser, input, metadata, context);
		}

		return reader;
	}

	private void excludeParser(final Class exclude) {
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
