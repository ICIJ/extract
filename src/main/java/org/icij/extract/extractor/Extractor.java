package org.icij.extract.extractor;

import java.io.FileNotFoundException;
import java.io.Reader;
import java.io.Writer;
import java.time.Duration;
import java.util.*;

import java.nio.file.Path;

import java.io.IOException;
import java.util.function.Function;

import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.EncryptedDocumentException;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TemporaryResources;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.CompositeParser;
import org.apache.tika.parser.DigestingParser;
import org.apache.tika.parser.EmptyParser;
import org.apache.tika.parser.html.HtmlMapper;
import org.apache.tika.parser.html.DefaultHtmlMapper;
import org.apache.tika.parser.ocr.TesseractOCRConfig;
import org.apache.tika.parser.ocr.TesseractOCRParser;
import org.apache.tika.parser.pdf.PDFParserConfig;
import org.apache.tika.extractor.EmbeddedDocumentExtractor;
import org.apache.tika.mime.MediaType;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.parser.utils.CommonsDigester;
import org.apache.tika.parser.utils.CommonsDigester.DigestAlgorithm;
import org.apache.tika.sax.ExpandedTitleContentHandler;
import org.apache.tika.sax.WriteOutContentHandler;
import org.icij.extract.document.Document;
import org.icij.extract.parser.*;
import org.icij.extract.report.Reporter;
import org.icij.extract.sax.HTML5Serializer;
import org.icij.extract.spewer.Spewer;
import org.icij.extract.spewer.SpewerException;
import org.icij.task.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;

/**
 * A reusable class that sets up Tika parsers based on runtime options.
 *
 * @since 1.0.0-beta
 */
public class Extractor {

	public enum OutputFormat {
		HTML, TEXT;

		public static OutputFormat parse(final String outputFormat) {
			return valueOf(outputFormat.toUpperCase(Locale.ROOT));
		}
	}

	public enum EmbedHandling {
		EMBED, CONCATENATE, SPAWN, IGNORE;

		public static EmbedHandling parse(final String outputFormat) {
			return valueOf(outputFormat.toUpperCase(Locale.ROOT));
		}

		public static EmbedHandling getDefault() {
			return SPAWN;
		}
	}

	private static final Logger logger = LoggerFactory.getLogger(Extractor.class);

	private boolean ocrDisabled = false;
	private Path workingDirectory = null;
	private DigestAlgorithm[] digestAlgorithms = null;

	private final TikaConfig config = TikaConfig.getDefaultConfig();
	private final TesseractOCRConfig ocrConfig = new TesseractOCRConfig();
	private final PDFParserConfig pdfConfig = new PDFParserConfig();

	private final Set<MediaType> excludedTypes = new HashSet<>();

	private OutputFormat outputFormat = OutputFormat.TEXT;
	private EmbedHandling embedHandling = EmbedHandling.getDefault();

	/**
	 * Create a new extractor, which will OCR images by default if Tesseract is available locally, extract inline
	 * images from PDF files and OCR them and use PDFBox's non-sequential PDF parser.
	 */
	public Extractor() {

		// Calculate the SHA256 digest by default.
		digestAlgorithms = new DigestAlgorithm[1];
		digestAlgorithms[0] = DigestAlgorithm.SHA256;

		// Run OCR on images contained within PDFs and not on pages.
		pdfConfig.setExtractInlineImages(true);
		pdfConfig.setOcrStrategy(PDFParserConfig.OCR_STRATEGY.NO_OCR);

		// By default, only the object IDs are used for determining uniqueness.
		// In scanned documents under test from the Panama registry, different embedded images had the same ID, leading to incomplete OCRing when uniqueness detection was turned on.
		pdfConfig.setExtractUniqueInlineImagesOnly(false);

		// Set a long OCR timeout by default, because Tika's is too short.
		setOcrTimeout(Duration.ofDays(1));
		ocrConfig.setEnableImageProcessing(0); // See TIKA-2167. Image processing causes OCR to fail.
	}

	public Extractor configure(final Options<String> options) {
		options.get("output-format").parse().asEnum(OutputFormat::parse).ifPresent(this::setOutputFormat);
		options.get("embed-handling").parse().asEnum(EmbedHandling::parse).ifPresent(this::setEmbedHandling);
		options.get("ocr-language").value().ifPresent(this::setOcrLanguage);
		options.get("ocr-timeout").parse().asDuration().ifPresent(this::setOcrTimeout);
		options.get("working-directory").parse().asPath().ifPresent(this::setWorkingDirectory);

		final Collection<DigestAlgorithm> digestAlgorithms = options.get("digest-method").values
				(DigestAlgorithm::valueOf);

		if (!digestAlgorithms.isEmpty()) {
			this.digestAlgorithms = digestAlgorithms.toArray(new DigestAlgorithm[digestAlgorithms.size()]);
		}

		if (options.get("ocr").parse().isOff()) {
			disableOcr();
		}

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
		this.digestAlgorithms = digestAlgorithms;
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
		final TikaInputStream input;

		// Use the the TikaInputStream.parse method that accepts a file, because this sets metadata properties like the
		// resource name and size.
		if (null != workingDirectory) {
			input = TikaInputStream.get(workingDirectory.resolve(document.getPath()), document.getMetadata());
		} else {
			input = TikaInputStream.get(document.getPath(), document.getMetadata());
		}

		return extract(document, input);
	}

	/**
	 * Extract and spew content from a document. Internally, as with {@link #extract(Document)}, this method creates a
	 * {@link TikaInputStream} from the path of the given document.
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
	 * {@link Reporter#save(Document, ExtractionStatus)}.
	 *
	 * @param document document to extract from
	 * @param spewer endpoint to write to
	 * @param reporter used to check whether the document should be skipped and save extraction status
	 */
	public void extract(final Document document, final Spewer spewer, final Reporter reporter) {
		if (null == reporter) {
			throw new IllegalArgumentException("The reporter must not be null.");
		}

		if (reporter.skip(document)) {
			logger.info(String.format("File already extracted; skipping: \"%s\".", document));
			return;
		}

		ExtractionStatus status = ExtractionStatus.SUCCEEDED;

		try {
			extract(document, spewer);
		} catch (Exception e) {
			status = status(e, document);
		}

		reporter.save(document, status);
	}

	/**
	 * Convert the given {@link Exception} into an {@link ExtractionStatus} for addition to a report.
	 *
	 * Logs an appropriate message depending on the exception.
	 *
	 * @param e the exception to convert and log
	 * @param document the document involved in the exception
	 * @return the resulting status
	 */
	private ExtractionStatus status(final Exception e, final Document document) {
		if (e instanceof SpewerException) {
			logger.error(String.format("The extraction result could not be outputted: \"%s\".", document), e);
			return ExtractionStatus.NOT_SAVED;
		}

		if (e instanceof FileNotFoundException) {
			logger.error(String.format("File not found: \"%s\". Skipping.", document), e);
			return ExtractionStatus.NOT_FOUND;
		}

		if (!(e instanceof IOException)) {
			logger.error(String.format("Unknown exception during extraction or output: \"%s\".", document), e);
			return ExtractionStatus.UNKNOWN_ERROR;
		}

		final Throwable cause = e.getCause();

		if (cause instanceof ExcludedMediaTypeException) {
			logger.warn(String.format("The document was not parsed because all of the parsers that handle it " +
					"were excluded: \"%s\".", document));
			return ExtractionStatus.EXCLUDED;
		}

		if (cause instanceof EncryptedDocumentException) {
			logger.warn(String.format("Skipping encrypted file: \"%s\".", document), e);
			return ExtractionStatus.NOT_DECRYPTED;
		}

		// TIKA-198: IOExceptions thrown by parsers will be wrapped in a TikaException.
		// This helps us differentiate input stream exceptions from output stream exceptions.
		// https://issues.apache.org/jira/browse/TIKA-198
		if (cause instanceof TikaException) {
			logger.error(String.format("The document could not be parsed: \"%s\".", document), e);
			return ExtractionStatus.NOT_PARSED;
		}

		logger.error(String.format("The document stream could not be read: \"%s\".", document), e);
		return ExtractionStatus.NOT_READ;
	}

	/**
	 * Extract from the given {@link TikaInputStream}, populating the given metadata object.
	 *
	 * @param input the stream to extract from
	 * @param document file that is being extracted from
	 */
	protected Reader extract(final Document document, final TikaInputStream input) throws IOException {
		final Metadata metadata = document.getMetadata();
		final ParseContext context = new ParseContext();
		final AutoDetectParser autoDetectParser = new AutoDetectParser(config);
		final Parser parser;

		if (null != digestAlgorithms && 0 != digestAlgorithms.length) {
			parser = new DigestingParser(autoDetectParser, new CommonsDigester(20 * 1024 * 1024,
					digestAlgorithms));
		} else {
			parser = autoDetectParser;
		}

		if (!ocrDisabled) {
			context.set(TesseractOCRConfig.class, ocrConfig);
		}

		context.set(PDFParserConfig.class, pdfConfig);
		autoDetectParser.setFallback(new ErrorParser(autoDetectParser, excludedTypes));

		// Only include "safe" tags in the HTML output from Tika's HTML parser.
		// This excludes script tags and objects.
		if (OutputFormat.HTML == outputFormat) {
			context.set(HtmlMapper.class, DefaultHtmlMapper.INSTANCE);
		}

		final Reader reader;

		if (OutputFormat.HTML == outputFormat && EmbedHandling.EMBED == embedHandling) {
			final TemporaryResources tmp = new TemporaryResources();
			final String uuid = UUID.randomUUID().toString();
			final String open = uuid + "/";
			final String close = "/" + uuid;

			context.set(Parser.class, EmptyParser.INSTANCE);
			context.set(EmbeddedDocumentExtractor.class, new EmbedLinker(document, tmp, open, close));
			context.set(TemporaryResources.class, tmp);

			// ParsingReader#close() method will get the TemporaryResources object from the context and close it.
			reader = new EmbeddingHTMLParsingReader(document, open, close, parser, input, metadata, context);
		} else {
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
				final TemporaryResources tmp = new TemporaryResources();

				context.set(Parser.class, parser);
				context.set(EmbeddedDocumentExtractor.class, new EmbedSpawner(document, context, tmp, handler));

				// Will be closed by the ParsingReader.
				context.set(TemporaryResources.class, tmp);
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
