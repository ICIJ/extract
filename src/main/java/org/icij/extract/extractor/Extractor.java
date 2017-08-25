package org.icij.extract.extractor;

import java.io.FileNotFoundException;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;

import java.io.IOException;
import java.util.function.Function;

import org.apache.commons.io.TaggedIOException;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.EncryptedDocumentException;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.*;
import org.apache.tika.parser.html.HtmlMapper;
import org.apache.tika.parser.html.DefaultHtmlMapper;
import org.apache.tika.parser.ocr.TesseractOCRConfig;
import org.apache.tika.parser.ocr.TesseractOCRParser;
import org.apache.tika.parser.pdf.PDFParserConfig;
import org.apache.tika.extractor.EmbeddedDocumentExtractor;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.parser.utils.CommonsDigester;
import org.apache.tika.parser.utils.CommonsDigester.DigestAlgorithm;
import org.apache.tika.sax.ExpandedTitleContentHandler;
import org.apache.tika.sax.WriteOutContentHandler;
import org.icij.extract.document.Document;
import org.icij.extract.parser.CachingTesseractOCRParser;
import org.icij.extract.parser.EmptyFileParser;
import org.icij.extract.parser.ParsingReader;
import org.icij.extract.report.Reporter;
import org.icij.extract.sax.HTML5Serializer;
import org.icij.extract.spewer.MetadataTransformer;
import org.icij.extract.spewer.Spewer;
import org.icij.task.Options;
import org.icij.task.annotation.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;

/**
 * A reusable class that sets up Tika parsers based on runtime options.
 *
 * @since 1.0.0-beta
 */
@Option(name = "digestMethod", description = "The hash digest method used for documents, for example \"SHA256\". May" +
		" be specified multiple times", parameter = "name")
@Option(name = "outputFormat", description = "Set the output format. Either \"text\" or \"HTML\". " +
		"Defaults to text output.", parameter = "type")
@Option(name = "embedHandling", description = "Set the embed handling mode. Either \"ignore\", " +
		"\"concatenate\" or \"spawn\". When set to concatenate, embeds are parsed and the output is " +
		"in-lined into the main output." +
		"Defaults to spawning, which spawns new documents for each embedded document encountered.", parameter = "type")
@Option(name = "embedOutput", description = "Path to a directory for outputting attachments en masse.",
		parameter = "path")
@Option(name = "ocrCache", description = "Output path for OCR cache files.", parameter = "path")
@Option(name = "ocrLanguage", description = "Set the languages used by Tesseract. Multiple  languages may be " +
		"specified, separated by plus characters. Tesseract uses 3-character ISO 639-2 language codes.", parameter =
		"language")
@Option(name = "ocrTimeout", description = "Set the timeout for the Tesseract process to finish e.g. \"5s\" or \"1m\"" +
		". Defaults to 12 hours.", parameter = "duration")
@Option(name = "ocr", description = "Enable or disable automatic OCR. On by default.")
public class Extractor {

	public enum OutputFormat {
		HTML, TEXT;

		public static OutputFormat parse(final String outputFormat) {
			return valueOf(outputFormat.toUpperCase(Locale.ROOT));
		}
	}

	public enum EmbedHandling {
		CONCATENATE, SPAWN, IGNORE;

		public static EmbedHandling parse(final String outputFormat) {
			return valueOf(outputFormat.toUpperCase(Locale.ROOT));
		}

		public static EmbedHandling getDefault() {
			return SPAWN;
		}
	}

	private static final Logger logger = LoggerFactory.getLogger(Extractor.class);

	private boolean ocrDisabled = false;
	private DigestingParser.Digester digester = null;

	private Parser defaultParser = TikaConfig.getDefaultConfig().getParser();
	private final TesseractOCRConfig ocrConfig = new TesseractOCRConfig();
	private final PDFParserConfig pdfConfig = new PDFParserConfig();

	private OutputFormat outputFormat = OutputFormat.TEXT;
	private EmbedHandling embedHandling = EmbedHandling.getDefault();
	private Path embedOutput = null;

	/**
	 * Create a new extractor, which will OCR images by default if Tesseract is available locally, extract inline
	 * images from PDF files and OCR them and use PDFBox's non-sequential PDF parser.
	 */
	public Extractor() {

		// Calculate the SHA256 digest by default.
		setDigestAlgorithms(DigestAlgorithm.SHA256);

		// Run OCR on images contained within PDFs and not on pages.
		pdfConfig.setExtractInlineImages(true);
		pdfConfig.setOcrStrategy(PDFParserConfig.OCR_STRATEGY.NO_OCR);

		// By default, only the object IDs are used for determining uniqueness.
		// In scanned documents under test from the Panama registry, different embedded images had the same ID, leading to incomplete OCRing when uniqueness detection was turned on.
		pdfConfig.setExtractUniqueInlineImagesOnly(false);

		// Set a long OCR timeout by default, because Tika's is too short.
		setOcrTimeout(Duration.ofDays(1));
		ocrConfig.setEnableImageProcessing(0); // See TIKA-2167. Image processing causes OCR to fail.

		// English text recognition by default.
		ocrConfig.setLanguage("eng");
	}

	public Extractor configure(final Options<String> options) {
		options.get("outputFormat").parse().asEnum(OutputFormat::parse).ifPresent(this::setOutputFormat);
		options.get("embedHandling").parse().asEnum(EmbedHandling::parse).ifPresent(this::setEmbedHandling);
		options.get("embedOutput").parse().asPath().ifPresent(this::setEmbedOutputPath);
		options.get("ocrLanguage").value().ifPresent(this::setOcrLanguage);
		options.get("ocrTimeout").parse().asDuration().ifPresent(this::setOcrTimeout);

		final Collection<DigestAlgorithm> digestAlgorithms = options.get("digestMethod").values
				(DigestAlgorithm::valueOf);

		if (!digestAlgorithms.isEmpty()) {
			setDigestAlgorithms(digestAlgorithms.toArray(new DigestAlgorithm[digestAlgorithms.size()]));
		}

		if (options.get("ocr").parse().isOff()) {
			disableOcr();
		}

		options.get("ocrCache").parse().asPath().ifPresent(path -> replaceParser(TesseractOCRParser.class,
				new CachingTesseractOCRParser(path)));

		return this;
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
	 * @return the embed handling mode.
	 */
	public EmbedHandling getEmbedHandling() {
		return embedHandling;
	}

	/**
	 * Set the output directory path for embed files.
	 *
	 * @param embedOutput the embed output path
	 */
	public void setEmbedOutputPath(final Path embedOutput) {
		this.embedOutput = embedOutput;
	}

	/**
	 * Get the output directory path for embed files.
	 *
	 * @return the embed output path.
	 */
	public Path getEmbedOutputPath() {
		return embedOutput;
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

	public void setDigestAlgorithms(final DigestAlgorithm... digestAlgorithms) {
		digester = new CommonsDigester(20 * 1024 * 1024, digestAlgorithms);
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
	 * This method will wrap the given {@link Document} in a {@link TikaInputStream} and return a {@link Reader}
	 * which can be used to initiate extraction on demand.
	 *
	 * Internally, this method uses {@link TikaInputStream#get} which ensures that the resource name and content
	 * length metadata properties are set automatically.
	 *
	 * @param document the file to extract from
	 * @return A {@link Reader} that can be used to read extracted text on demand.
	 */
	public Reader extract(final Document document) throws IOException {

		// Use the the TikaInputStream.parse method that accepts a file, because this sets metadata properties like the
		// resource name and size.
		return extract(document, TikaInputStream.get(document.getPath(), document.getMetadata()));
	}

	/**
	 * Extract and spew content from a document. Internally, as with {@link #extract(Document)},
	 * this method creates a {@link TikaInputStream} from the path of the given document.
	 *
	 * @param document document to extract from
	 * @param spewer endpoint to write to
	 * @throws IOException if there was an error reading or writing the document
	 */
	public void extract(final Document document, final Spewer spewer) throws IOException {
		try (final Reader reader = extract(document)) {
			spewer.write(document, reader);
		}
	}

	/**
	 * Extract and spew content from a document. This method is the same as {@link #extract(Document, Spewer)} with
	 * the exception that the document will be skipped if the reporter returns {@literal false} for a call to
	 * {@link Reporter#skip(Document)}.
	 *
	 * If the document is not skipped, then the result of the extraction is passed to the reporter in a call to
	 * {@link Reporter#save(Document, ExtractionStatus, Exception)}.
	 *
	 * @param document document to extract from
	 * @param spewer endpoint to write to
	 * @param reporter used to check whether the document should be skipped and save extraction status
	 */
	public void extract(final Document document, final Spewer spewer, final Reporter reporter) {
		Objects.requireNonNull(reporter);

		if (reporter.skip(document)) {
			logger.info(String.format("File already extracted; skipping: \"%s\".", document));
			return;
		}

		ExtractionStatus status = ExtractionStatus.SUCCESS;
		Exception exception = null;

		try {
			extract(document, spewer);
		} catch (final Exception e) {
			status = status(e, spewer);
			log(e, status, document);
			exception = e;
		}

		// For tagged IO exceptions, discard the tag, which is either unwanted or not serializable.
		if (null != exception && (exception instanceof TaggedIOException)) {
			exception = ((TaggedIOException) exception).getCause();
		}

		reporter.save(document, status, exception);
	}

	private void log(final Exception e, final ExtractionStatus status, final Document document) {
		switch (status) {
			case FAILURE_NOT_SAVED:
				logger.error(String.format("The extraction result could not be outputted: \"%s\".", document),
						e.getCause());
				break;
			case FAILURE_NOT_FOUND:
				logger.error(String.format("File not found: \"%s\".", document), e);
				break;
			case FAILURE_NOT_DECRYPTED:
				logger.warn(String.format("Skipping encrypted file: \"%s\".", document), e);
				break;
			case FAILURE_NOT_PARSED:
				logger.error(String.format("The document could not be parsed: \"%s\".", document), e);
				break;
			case FAILURE_UNREADABLE:
				logger.error(String.format("The document stream could not be read: \"%s\".", document), e);
				break;
			default:
				logger.error(String.format("Unknown exception during extraction or output: \"%s\".", document), e);
				break;
		}
	}

	/**
	 * Convert the given {@link Exception} into an {@link ExtractionStatus} for addition to a report.
	 *
	 * Logs an appropriate message depending on the exception.
	 *
	 * @param e the exception to convert and log
	 * @return the resulting status
	 */
	private ExtractionStatus status(final Exception e, final Spewer spewer) {
		if (TaggedIOException.isTaggedWith(e, spewer)) {
			return ExtractionStatus.FAILURE_NOT_SAVED;
		}

		if (TaggedIOException.isTaggedWith(e, MetadataTransformer.class)) {
			return ExtractionStatus.FAILURE_NOT_PARSED;
		}

		if (e instanceof FileNotFoundException) {
			return ExtractionStatus.FAILURE_NOT_FOUND;
		}

		if (!(e instanceof IOException)) {
			return ExtractionStatus.FAILURE_UNKNOWN;
		}

		final Throwable cause = e.getCause();

		if (cause instanceof EncryptedDocumentException) {
			return ExtractionStatus.FAILURE_NOT_DECRYPTED;
		}

		// TIKA-198: IOExceptions thrown by parsers will be wrapped in a TikaException.
		// This helps us differentiate input stream exceptions from output stream exceptions.
		// https://issues.apache.org/jira/browse/TIKA-198
		if (cause instanceof TikaException) {
			return ExtractionStatus.FAILURE_NOT_PARSED;
		}

		return ExtractionStatus.FAILURE_UNREADABLE;
	}

	/**
	 * Create a pull-parser from the given {@link TikaInputStream}.
	 *
	 * @param input the stream to extract from
	 * @param document file that is being extracted from
	 * @return A pull-parsing reader.
	 */
	protected Reader extract(final Document document, final TikaInputStream input) throws IOException {
		final Metadata metadata = document.getMetadata();
		final ParseContext context = new ParseContext();
		final AutoDetectParser autoDetectParser = new AutoDetectParser(defaultParser);
		final Parser parser;

		if (null != digester) {
			parser = new DigestingParser(autoDetectParser, digester);
		} else {
			parser = autoDetectParser;
		}

		if (!ocrDisabled) {
			context.set(TesseractOCRConfig.class, ocrConfig);
		}

		context.set(PDFParserConfig.class, pdfConfig);

		// Set a fallback parser that outputs an empty document for empty files,
		// otherwise throws an exception.
		autoDetectParser.setFallback(EmptyFileParser.INSTANCE);

		// Only include "safe" tags in the HTML output from Tika's HTML parser.
		// This excludes script tags and objects.
		context.set(HtmlMapper.class, DefaultHtmlMapper.INSTANCE);

		final Reader reader;
		final Function<Writer, ContentHandler> handler;

		if (OutputFormat.HTML == outputFormat) {
			handler = (writer) -> new ExpandedTitleContentHandler(new HTML5Serializer(writer));
		} else {

			// The default BodyContentHandler is used when constructing the ParsingReader for text output, but
			// because only the body of embeds is pushed to the content handler further down the line, we can't
			// expect a body tag.
			handler = WriteOutContentHandler::new;
		}

		if (EmbedHandling.SPAWN == embedHandling) {
			context.set(Parser.class, parser);
			context.set(EmbeddedDocumentExtractor.class, new EmbedSpawner(document, context, embedOutput, handler));
		} else if (EmbedHandling.CONCATENATE == embedHandling) {
			context.set(Parser.class, parser);
			context.set(EmbeddedDocumentExtractor.class, new EmbedParser(document, context));
		} else {
			context.set(Parser.class, EmptyParser.INSTANCE);
			context.set(EmbeddedDocumentExtractor.class, new EmbedBlocker());
		}

		if (OutputFormat.HTML == outputFormat) {
			reader = new ParsingReader(parser, input, metadata, context, handler);
		} else {
			reader = new ParsingReader(parser, input, metadata, context);
		}

		return reader;
	}

	private void excludeParser(final Class<? extends Parser> exclude) {
		replaceParser(exclude, null);
	}

	private void replaceParser(final Class<? extends Parser> exclude, final Parser replacement) {
		if (defaultParser instanceof CompositeParser) {
			final CompositeParser composite = (CompositeParser) defaultParser;
			final List<Parser> parsers = new ArrayList<>();

			composite.getAllComponentParsers().forEach(parser -> {
				if (parser.getClass().equals(exclude) || exclude.isAssignableFrom(parser.getClass())) {
					if (null != replacement) {
						parsers.add(replacement);
					}
				} else {
					parsers.add(parser);
				}
			});

			defaultParser = new CompositeParser(composite.getMediaTypeRegistry(), parsers);
		}
	}
}
