package org.icij.extract.extractor;

import java.lang.reflect.InvocationTargetException;
import org.apache.commons.io.TaggedIOException;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.EncryptedDocumentException;
import org.apache.tika.exception.TikaException;
import org.apache.tika.extractor.DocumentSelector;
import org.apache.tika.extractor.EmbeddedDocumentExtractor;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.CompositeParser;
import org.apache.tika.parser.DigestingParser;
import org.apache.tika.parser.EmptyParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.digestutils.CommonsDigester;
import org.apache.tika.parser.digestutils.CommonsDigester.DigestAlgorithm;
import org.apache.tika.parser.html.DefaultHtmlMapper;
import org.apache.tika.parser.html.HtmlMapper;
import org.apache.tika.parser.ocr.TesseractOCRParser;
import org.apache.tika.parser.pdf.PDFParserConfig;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.ExpandedTitleContentHandler;
import org.apache.tika.utils.ServiceLoaderUtils;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.PathIdentifier;
import org.icij.extract.document.TikaDocument;
import org.icij.extract.ocr.OCRConfigAdapter;
import org.icij.extract.ocr.OCRConfigRegistry;
import org.icij.extract.ocr.Tess4JOCRParser;
import org.icij.extract.ocr.TesseractOCRConfigAdapter;
import org.icij.extract.parser.CacheParserDecorator;
import org.icij.extract.parser.FallbackParser;
import org.icij.extract.parser.HTML5Serializer;
import org.icij.extract.parser.ParsingReaderWithContentHandler;
import org.icij.extract.report.Reporter;
import org.icij.spewer.MetadataTransformer;
import org.icij.spewer.Spewer;
import org.icij.task.Options;
import org.icij.task.annotation.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.lang.System.currentTimeMillis;
import static org.icij.extract.LambdaExceptionUtils.rethrowFunction;

/**
 * A reusable class that sets up Tika parsers based on runtime options.
 *
 * @since 1.0.0-beta
 */
@Option(name = "digestAlgorithm", description = "The hash digest method used for documents, for example \"SHA256\".", parameter = "name")
@Option(name = "digestProjectName", description = "Include the given project name in the document hash.", parameter = "name")
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
@Option(name = "ocrType", description = "Name of the OCR to use TESSERACT, TESS4J")
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
    protected OCRConfigAdapter ocrConfig;
    private final PDFParserConfig pdfConfig = new PDFParserConfig();
    private final DocumentFactory documentFactory;

    private OutputFormat outputFormat = OutputFormat.TEXT;
    private EmbedHandling embedHandling = EmbedHandling.getDefault();
    private Path embedOutput = null;

    /**
     * Create a new extractor, which will OCR images by default if Tesseract is available locally, extract inline
     * images from PDF files and OCR them and use PDFBox's non-sequential PDF parser.
     */
    public Extractor(final DocumentFactory factory) {
        this.documentFactory = factory;
        // Calculate the SHA256 digest by default.
        setDigestAlgorithm(DigestAlgorithm.SHA256.toString());

        // Run OCR on images contained within PDFs and not on pages.
        pdfConfig.setExtractInlineImages(true);
        pdfConfig.setOcrStrategy(PDFParserConfig.OCR_STRATEGY.NO_OCR);

        // By default, only the object IDs are used for determining uniqueness.
        // In scanned documents under test from the Panama registry, different embedded images had the same ID, leading to incomplete OCRing when uniqueness detection was turned on.
        pdfConfig.setExtractUniqueInlineImagesOnly(false);

        // English text recognition by default.
        ocrConfig = new TesseractOCRConfigAdapter();
        ocrConfig.setLanguages("eng");
        ocrConfig.setOcrTimeout(Duration.ofDays(1));
    }

    public Extractor() {
        this(new DocumentFactory().withIdentifier(new PathIdentifier()));
    }

    public Extractor configure(final Options<String> options) {
        options.get("outputFormat", "TEXT").parse().asEnum(OutputFormat::parse).ifPresent(this::setOutputFormat);
        options.get("embedHandling", "SPAWN").parse().asEnum(EmbedHandling::parse).ifPresent(this::setEmbedHandling);
        options.get("ocrType", String.valueOf(OCRConfigRegistry.TESSERACT))
            .parse()
            .asEnum(OCRConfigRegistry::parse)
            .map(rethrowFunction(OCRConfigRegistry::newAdapter))
            .ifPresent(this::setOcrConfig);
        options.get("ocrLanguage", "eng").value().ifPresent(this::setOcrLanguage);
        options.get("ocrTimeout", "12h").parse().asDuration().ifPresent(this::setOcrTimeout);
        options.valueIfPresent("embedOutput").ifPresent(embedOutput -> setEmbedOutputPath(Paths.get(embedOutput)));

        String algorithm = options.valueIfPresent("digestAlgorithm").orElse("SHA-256");
        setDigestAlgorithm(algorithm);

        options.valueIfPresent("digestProjectName")
                .ifPresent(digestProjectName -> this.setDigester(new UpdatableDigester(digestProjectName, algorithm)));

        if (options.get("ocr", String.valueOf(!this.ocrDisabled)).parse().isOff()) {
            disableOcr();
        }

        options.valueIfPresent("ocrCache").ifPresent(
            path -> replaceParser(ocrConfig.getParserClass(), parser -> new CacheParserDecorator(parser, Paths.get(path)))
        );
        logger.info("extractor configured with digester {} and {}", digester.getClass(), documentFactory);

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

    public void setOcrConfig(final OCRConfigAdapter<?, ?> ocrConfig) {
        this.ocrConfig = ocrConfig;
        Parser ocrParser;
        Class<?> parserClass = ocrConfig.getParserClass();
        try {
            ocrParser = (Parser) parserClass.getConstructor().newInstance();
        } catch (IllegalAccessException e) {
            throw new RuntimeException(parserClass + " no-arg constructor is not accessible");
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(parserClass + " has no no-arg constructor");
        } catch (InvocationTargetException | InstantiationException e) {
            throw new RuntimeException("failed to instanciate " + parserClass + " using has no no-arg constructor");
        }
        for (OCRConfigRegistry c: OCRConfigRegistry.values()) {
            replaceParser(c.newAdapter().getParserClass(), parser -> ocrParser);
        }
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
        ocrConfig.setLanguages(ocrLanguage.split("\\+"));
    }

    /**
     * Instructs Tesseract to attempt OCR for no longer than the given duration in seconds.
     *
     * @param ocrTimeout the duration in seconds
     */
    private void setOcrTimeout(final int ocrTimeout) {
        ocrConfig.setParsingTimeoutS(ocrTimeout);
    }

    /**
     * Instructs Tesseract to attempt OCR for no longer than the given duration.
     *
     * @param duration the duration before timeout
     */
    public void setOcrTimeout(final Duration duration) {
        setOcrTimeout(Math.toIntExact(duration.getSeconds()));
    }

    public void setDigestAlgorithm(final String digestAlgorithm) {
        setDigester(new CommonsDigester(20 * 1024 * 1024, digestAlgorithm.replace("-", "")));
    }

    public void setDigester(final DigestingParser.Digester digester) {
        this.digester = digester;
    }

    /**
     * Disable OCR. This method only has an effect if Tesseract is installed.
     */
    public void disableOcr() {
        if (!ocrDisabled) {
            excludeParser(ocrConfig.getParserClass());
            ocrDisabled = true;
            pdfConfig.setExtractInlineImages(false);
        }
    }

    /**
     * Extract and spew content from a document. Internally, as with {@link #extract(Path)},
     * this method creates a {@link TikaInputStream} from the path of the given document.
     *
     * @param path   document to extract from
     * @param spewer endpoint to write to
     * @throws IOException if there was an error reading or writing the document
     */
    public void extract(final Path path, final Spewer spewer) throws IOException {
        long before = currentTimeMillis();
        TikaDocument document = extract(path);
        logger.info("{} extracted in {}ms", path, currentTimeMillis() - before);
        spewer.write(document);
    }

    /**
     * Extract and spew content from a document. This method is the same as {@link #extract(Path, Spewer)} with
     * the exception that the document will be skipped if the reporter returns {@literal false} for a call to
     * {@link Reporter#skip(Path)}.
     * <p>
     * If the document is not skipped, then the result of the extraction is passed to the reporter in a call to
     * {@link Reporter#save(Path, ExtractionStatus, Exception)}.
     *
     * @param path     document to extract from
     * @param spewer   endpoint to write to
     * @param reporter used to check whether the document should be skipped and save extraction status
     */
    public void extract(final Path path, final Spewer spewer, final Reporter reporter) {
        Objects.requireNonNull(reporter);

        if (reporter.skip(path)) {
            logger.info(String.format("File already extracted; skipping: \"%s\".", path));
            return;
        }

        ExtractionStatus status = ExtractionStatus.SUCCESS;
        Exception exception = null;

        try {
            extract(path, spewer);
        } catch (final Exception e) {
            status = status(e, spewer);
            log(e, status, path);
            exception = e;
        }

        // For tagged IO exceptions, discard the tag, which is either unwanted or not serializable.
        if ((exception instanceof TaggedIOException)) {
            exception = ((TaggedIOException) exception).getCause();
        }

        reporter.save(path, status, exception);
    }

    private void log(final Exception e, final ExtractionStatus status, final Path file) {
        switch (status) {
            case FAILURE_NOT_SAVED:
                logger.error(String.format("The extraction result could not be outputted: \"%s\".", file),
                        e.getCause());
                break;
            case FAILURE_NOT_FOUND:
                logger.error(String.format("File not found: \"%s\".", file), e);
                break;
            case FAILURE_NOT_DECRYPTED:
                logger.warn(String.format("Skipping encrypted file: \"%s\".", file), e);
                break;
            case FAILURE_NOT_PARSED:
                logger.error(String.format("The file could not be parsed: \"%s\".", file), e);
                break;
            case FAILURE_UNREADABLE:
                logger.error(String.format("The file stream could not be read: \"%s\".", file), e);
                break;
            default:
                logger.error(String.format("Unknown exception during extraction or output: \"%s\".", file), e);
                break;
        }
    }

    /**
     * Convert the given {@link Exception} into an {@link ExtractionStatus} for addition to a report.
     * <p>
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
     * @param path the stream to extract from
     * @return A pull-parsing reader.
     */
    public TikaDocument extract(final Path path) throws IOException {
        final Function<Writer, ContentHandler> handler;
        if (OutputFormat.HTML == outputFormat) {
            handler = (writer) -> new ExpandedTitleContentHandler(new HTML5Serializer(writer));
        } else {
            handler = BodyContentHandler::new;
        }

        return getTikaDocument(path, handler, metadata -> true);
    }

    public List<Pair<Long, Long>> extractPageIndices(final Path path) throws IOException {
        return extractPageIndices(path, metadata -> true);
    }

    public List<String> extractPages(final Path path) throws IOException {
        return extractPages(path, metadata -> true);
    }

    public List<String> extractPages(Path path, DocumentSelector documentSelector) throws IOException {
        boolean notEmbedded = documentSelector.select(new Metadata());
        PagesContentHandler contentHandler = createContentHandlerForPages(notEmbedded);
        final Function<Writer, ContentHandler> handlerProvider = (writer) -> contentHandler;
        TikaDocument tikaDocument = getTikaDocument(path, handlerProvider, documentSelector);
        try (final Reader reader = tikaDocument.getReader()) {
            Spewer.copy(reader, Writer.nullWriter());
        }
        return contentHandler.getPages();
    }

    public List<Pair<Long, Long>> extractPageIndices(final Path path, DocumentSelector documentSelector) throws IOException {
        boolean notEmbedded = documentSelector.select(new Metadata());
        PageIndicesContentHandler contentHandler = createContentHandlerForPageIndices(notEmbedded);
        final Function<Writer, ContentHandler> handlerProvider = (writer) -> contentHandler;
        TikaDocument tikaDocument = getTikaDocument(path, handlerProvider, documentSelector);
        try (final Reader reader = tikaDocument.getReader()) {
            Spewer.copy(reader, Writer.nullWriter());
        }
        return contentHandler.getPageIndices();
    }

    private PagesContentHandler createContentHandlerForPages(boolean notEmbedded) {
        if (OutputFormat.HTML == outputFormat) {
            return new PagesContentHandler(new ExpandedTitleContentHandler(new HTML5Serializer(Writer.nullWriter())), notEmbedded);
        } else {
            return new PagesContentHandler(new BodyContentHandler(Writer.nullWriter()), notEmbedded);
        }
    }

    private PageIndicesContentHandler createContentHandlerForPageIndices(boolean notEmbedded) {
        if (OutputFormat.HTML == outputFormat) {
            return new PageIndicesContentHandler(new ExpandedTitleContentHandler(new HTML5Serializer(Writer.nullWriter())), notEmbedded);
        } else {
            return new PageIndicesContentHandler(new BodyContentHandler(Writer.nullWriter()), notEmbedded);
        }
    }

    private TikaDocument getTikaDocument(Path path, final Function<Writer, ContentHandler> handlerProvider, DocumentSelector documentSelector) throws IOException {
        final TikaDocument rootDocument = documentFactory.create(path);
        TikaInputStream tikaInputStream = TikaInputStream.get(path, rootDocument.getMetadata());
        final ParseContext context = new ParseContext();
        final AutoDetectParser autoDetectParser = new AutoDetectParser(defaultParser);

        // Set a fallback parser that outputs an empty tikaDocument for empty files,
        // otherwise throws an exception.
        autoDetectParser.setFallback(FallbackParser.INSTANCE);
        final Parser parser;

        if (null != digester) {
            parser = new DigestingParser(autoDetectParser, digester, false);
        } else {
            parser = autoDetectParser;
        }

        if (!ocrDisabled) {
            context.set(ocrConfig.getParserClass(), ocrConfig.getConfig());
        }

        context.set(PDFParserConfig.class, pdfConfig);
        context.set(DocumentSelector.class, documentSelector);

        // Only include "safe" tags in the HTML output from Tika's HTML parser.
        // This excludes script tags and objects.
        context.set(HtmlMapper.class, DefaultHtmlMapper.INSTANCE);

        if (EmbedHandling.SPAWN == embedHandling) {
            context.set(Parser.class, parser);
            context.set(EmbeddedDocumentExtractor.class, new EmbedSpawner(rootDocument, context, embedOutput, handlerProvider));
        } else if (EmbedHandling.CONCATENATE == embedHandling) {
            context.set(Parser.class, parser);
            context.set(EmbeddedDocumentExtractor.class, new EmbedParser(rootDocument, context));
        } else {
            context.set(Parser.class, EmptyParser.INSTANCE);
            context.set(EmbeddedDocumentExtractor.class, new EmbedBlocker());
        }

        final Reader reader = new ParsingReaderWithContentHandler(parser, tikaInputStream, rootDocument.getMetadata(), context, handlerProvider);
        rootDocument.setReader(reader);

        return rootDocument;
    }

    private void excludeParser(final Class<? extends Parser> exclude) {
        replaceParser(exclude, null);
    }

    public static CompositeParser replaceParser(Parser parser, final Class<? extends Parser> exclude, final Function<Parser, Parser> parserFn) {
        if (parser instanceof CompositeParser composite) {
            final List<Parser> parsers = new ArrayList<>();
            getAllSubParsers(composite).forEach(p -> {
                if (p.getClass().equals(exclude) || exclude.isAssignableFrom(p.getClass())) {
                    if (parserFn != null) {
                        parsers.add(parserFn.apply(p));
                    }
                } else {
                    parsers.add(p);
                }
            });
            ServiceLoaderUtils.sortLoadedClasses(parsers);
            //reverse the order of parsers so that custom ones come last
            //this will prevent them from being overwritten in getParsers(ParseContext ..)
            Collections.reverse(parsers);
            return new CompositeParser(composite.getMediaTypeRegistry(), parsers);
        }
        return null;
    }

    public static Stream<Parser> getAllSubParsers(CompositeParser compositeParser) {
        return compositeParser.getAllComponentParsers().stream().flatMap(
            sub -> {
                if (sub instanceof CompositeParser composite) {
                    return getAllSubParsers(composite);
                } else {
                    return Stream.of(sub);
                }
            }
        );
    }

    private void replaceParser(final Class<? extends Parser> exclude, final Function<Parser, Parser> parserFn) {
        defaultParser = replaceParser(defaultParser, exclude, parserFn);
    }
}
