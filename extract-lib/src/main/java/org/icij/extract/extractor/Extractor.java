package org.icij.extract.extractor;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.io.TaggedIOException;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.EncryptedDocumentException;
import org.apache.tika.exception.TikaException;
import org.apache.tika.extractor.DocumentSelector;
import org.apache.tika.extractor.EmbeddedDocumentExtractor;
import org.apache.tika.io.TemporaryResources;
import org.apache.tika.io.TikaInputStream;
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
import org.apache.tika.parser.microsoft.pst.OutlookPSTParser;
import org.apache.tika.parser.ocr.TesseractOCRConfig;
import org.apache.tika.parser.pdf.PDFParserConfig;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.ExpandedTitleContentHandler;
import org.apache.tika.utils.ServiceLoaderUtils;
import org.icij.extract.document.DocumentFactory;
import org.icij.extract.document.PathIdentifier;
import org.icij.extract.document.TikaDocument;
import org.icij.extract.ocr.ImageIOTranscodingOCRParser;
import org.icij.extract.ocr.OCRConfigAdapter;
import org.icij.extract.ocr.OCRConfigRegistry;
import org.icij.extract.ocr.OCRParserAdapter;
import org.icij.extract.ocr.TesseractOCRConfigAdapter;
import org.icij.extract.parser.CacheParserDecorator;
import org.icij.extract.parser.FallbackParser;
import org.icij.extract.parser.HTML5Serializer;
import org.icij.extract.parser.ParsingReaderWithContentHandler;
import org.icij.extract.parser.ResourceClosingReader;
import org.icij.extract.parser.ResilientOutlookPSTParser;
import org.icij.extract.report.Reporter;
import org.icij.spewer.MetadataTransformer;
import org.icij.spewer.Spewer;
import org.icij.spewer.SpewSink;
import org.icij.spewer.StreamingSpewCoordinator;
import org.icij.task.Options;
import org.icij.task.annotation.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.lang.System.currentTimeMillis;
import static java.util.Optional.ofNullable;
import static org.icij.extract.extractor.ArtifactUtils.getEmbeddedPath;

/**
 * A reusable class that sets up Tika parsers based on runtime options.
 *
 * <p>This class is {@link AutoCloseable}. Callers that construct an {@code Extractor} SHOULD
 * call {@link #close()} (or use try-with-resources) to release the OCR pool and heartbeat
 * scheduler. Both are created lazily: an {@code Extractor} that performs no deferred-OCR
 * extraction holds no extra threads and is safe to discard without closing, but closing is
 * still strongly recommended to prevent a pool from lingering if OCR was used.
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
@Option(name = "ocrStrategy", description = "Set the PDF OCR strategy. One of \"NO_OCR\" " +
        "(default), \"AUTO\", \"OCR_AND_TEXT_EXTRACTION\" or \"OCR_ONLY\". Any rendering " +
        "strategy OCRs whole pages and disables inline-image extraction, which is required to " +
        "correctly extract scanned/MRC PDFs. Defaults to NO_OCR.", parameter = "strategy")
@Option(name = "ocrTimeout", description = "Set the timeout for the Tesseract process to finish e.g. \"5s\" or \"1m\"" +
        ". Defaults to 12 hours.", parameter = "duration")
@Option(name = "parseTimeout", description = "Wall-clock timeout for a single document's parse " +
        "and output, e.g. \"30m\" or \"24h\". Set to 0 to disable. Defaults to 24h.", parameter = "duration")
@Option(name = "ocr", description = "Enable or disable automatic OCR. On by default.")
@Option(name = "ocrType", description = "Name of the OCR to use TESSERACT, TESS4J")
@Option(name = "embedMemoryBudgetMb", description = "Maximum megabytes of embedded-document " +
        "extracted text to hold in memory before overflowing to temp files. Defaults to 64.",
        parameter = "megabytes")
@Option(name = "embedMemoryPressureThreshold", description = "Heap occupancy ratio (0-1) above which " +
        "embedded-document text spills to temp files early, regardless of the byte budget, to keep " +
        "extraction within the available heap on very large containers. Defaults to 0.7; set to 0 or 1 to disable.",
        parameter = "ratio")
@Option(name = "ocrParallelism", description = "Number of OCR tasks run in parallel across all " +
        "extraction threads. Defaults to the number of available processors. Set to 1 for serial OCR.",
        parameter = "count")
@Option(name = "ocrFanout", description = "Enable parallel OCR fan-out for image attachments. " +
        "On by default. When off, images OCR inline on the parse thread.")
@Option(name = "ocrMinImageBytes", description = "Image embeds smaller than this many bytes OCR " +
        "inline instead of via the pool. Defaults to 0 (always fan out).", parameter = "bytes")
@Option(name = "progressHeartbeatInterval", description = "Interval between extraction progress " +
        "heartbeat log lines, e.g. \"60s\". Set to 0 to disable. Defaults to 60s.", parameter = "duration")
@Option(name = "streamingSpew", description = "Write embedded documents to the spewer as they are " +
        "parsed, instead of buffering the whole tree and writing it afterwards. On by default; set " +
        "to false to fall back to the legacy buffer-then-walk path.")
@Option(name = "spewQueueCapacity", description = "Maximum number of parsed-but-not-yet-written " +
        "embedded documents held in the streaming-spew queue before the parse thread blocks " +
        "(backpressure). Defaults to 1000.", parameter = "count")
@Option(name = "pstFolderFanout", description = "Parse the folders of a single PST/OST mailbox " +
        "in parallel across a shared bounded pool, instead of one thread per file. Embed ids, " +
        "digests and artifact filenames are byte-identical to the serial walk.")
@Option(name = "pstParseParallelism", description = "Number of PST/OST folder-walk tasks run in " +
        "parallel across all in-flight mailboxes. Separate from ocrParallelism.")
@Option(name = "legacyUntitledNaming", description = "Name nameless non-inline embeds with the " +
        "pre-9.x global untitled_N counter instead of the per-parent scheme, for on-demand " +
        "resolution of corpora indexed before the per-parent change. Serial mode only.")
@Option(name = "maxEmbedDepth", description = "Maximum nesting depth of embedded documents to " +
        "descend into. Embeds deeper than this are skipped (recorded, not parsed) to guard against " +
        "decompression-bomb archives. Defaults to 20. Set to 0 to disable.", parameter = "count")
public class Extractor implements AutoCloseable {

    public static final String PAGES_JSON = "pages.json";

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
    protected OCRConfigAdapter<? extends Parser> ocrConfig;
    private final PDFParserConfig pdfConfig = new PDFParserConfig();
    private final DocumentFactory documentFactory;
    private OutputFormat outputFormat = OutputFormat.TEXT;
    private EmbedHandling embedHandling = EmbedHandling.getDefault();
    private Path embedOutput = null;
    private long embedMemoryBudgetBytes = 64L * 1024 * 1024;
    private double embedMemoryPressureThreshold = 0.7;
    private Duration parseTimeout = Duration.ofDays(1);
    private final ExecutorService parseExecutor = Executors.newCachedThreadPool(new ThreadFactory() {
        private final AtomicInteger counter = new AtomicInteger();
        @Override
        public Thread newThread(final Runnable r) {
            final Thread thread = new Thread(r, "extract-watchdog-" + counter.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        }
    });
    private int ocrParallelism = Runtime.getRuntime().availableProcessors();
    private boolean ocrFanout = true;
    private long ocrMinImageBytes = 0L;
    private Duration progressHeartbeatInterval = Duration.ofSeconds(60);
    private boolean streamingSpew = true;
    // Bounded spew queue: caps how many ready-but-unwritten embeds (and thus their buffered text)
    // are held in flight, providing backpressure on the parse thread when the spewer lags.
    private int spewQueueCapacity = 1000;
    // Null until first use; created lazily by ocrExecutor() to avoid leaking threads in
    // Extractors that never actually defer OCR (OCR disabled, fanout off, or no eligible image).
    // Volatile so that close() on any thread sees the value written by the synchronized creator.
    private volatile ExecutorService ocrExecutor = null;
    private boolean pstFolderFanout = true;
    private int pstParseParallelism = Runtime.getRuntime().availableProcessors();
    private boolean legacyUntitledNaming = false;
    private int maxEmbedDepth = EmbedSpawner.DEFAULT_MAX_EMBED_DEPTH;
    // Null until first use; created lazily by parseExecutor(), mirroring the OCR pool.
    private volatile ExecutorService pstParseExecutor = null;
    private ExtractionProgressTracker progressTracker;

    /**
     * Create a new extractor, which will OCR images by default if Tesseract is available locally, extract inline
     * images from PDF files and OCR them and use PDFBox's non-sequential PDF parser.
     */
    public Extractor(final DocumentFactory factory) {
        this(factory, null);
    }

    public Extractor(Options<String> options) {
        this(new DocumentFactory().withIdentifier(new PathIdentifier()), options);
    }

    public Extractor(final DocumentFactory factory, Options<String> options) {
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
        this.configure(Optional.ofNullable(options).orElse(Options.from(Map.of())));
        // Replace Tika's stock OutlookPSTParser, which silently aborts the rest
        // of a PST when one message fails, with the resilient parser.
        replaceParser(OutlookPSTParser.class, parser -> new ResilientOutlookPSTParser());
        // The OCR pool is created lazily on first deferred-OCR use (see ocrExecutor()).
        // progressTracker.start() is deferred to the first begin() call so an Extractor
        // that is never used starts no scheduler thread.
        this.progressTracker = new ExtractionProgressTracker(progressHeartbeatInterval);
        this.progressTracker.addListener(new LoggingProgressListener());
    }

    public Extractor() {
        this(new DocumentFactory().withIdentifier(new PathIdentifier()));
    }

    private void configure(final Options<String> options) {
        options.get("outputFormat", "TEXT").parse().asEnum(OutputFormat::parse).ifPresent(this::setOutputFormat);
        options.get("embedHandling", "SPAWN").parse().asEnum(EmbedHandling::parse).ifPresent(this::setEmbedHandling);
        options.get("ocrType", String.valueOf(OCRConfigRegistry.TESSERACT))
            .parse()
            .asEnum(OCRConfigRegistry::parse)
            .map(OCRConfigRegistry::buildAdapter)
            .ifPresent(this::setOcrConfig);
        options.get("ocrLanguage", "eng").value().ifPresent(this::setOcrLanguage);
        options.get("ocrStrategy", "NO_OCR").value().ifPresent(this::setOcrStrategy);
        options.get("ocrTimeout", "12h").parse().asDuration().ifPresent(this::setOcrTimeout);
        options.get("parseTimeout", "24h").parse().asDuration().ifPresent(this::setParseTimeout);
        options.valueIfPresent("embedOutput").ifPresent(embedOutput -> setEmbedOutputPath(Paths.get(embedOutput)));
        options.get("embedMemoryBudgetMb", "64").parse().asInteger()
                .ifPresent(mb -> setEmbedMemoryBudgetBytes(mb * 1024L * 1024L));
        options.valueIfPresent("embedMemoryPressureThreshold")
                .ifPresent(ratio -> setEmbedMemoryPressureThreshold(Double.parseDouble(ratio)));

        String algorithm = options.valueIfPresent("digestAlgorithm").orElse("SHA-256");
        setDigestAlgorithm(algorithm);

        options.valueIfPresent("digestProjectName")
                .ifPresent(digestProjectName -> this.setDigester(new UpdatableDigester(digestProjectName, algorithm)));

        if (options.get("ocr", String.valueOf(!this.ocrDisabled)).parse().isOff()) {
            disableOcr();
        }
        options.valueIfPresent("ocrCache").ifPresent(path -> {
            replaceParser(ocrConfig.getParserClass(), parser -> new CacheParserDecorator(parser, Paths.get(path)));
            // Cache the transcoding fallback too; otherwise JBIG2 (and other transcoded) images
            // re-run OCR on every extraction because the fallback holds its own OCR-parser reference.
            replaceParser(ImageIOTranscodingOCRParser.class, parser -> new CacheParserDecorator(parser, Paths.get(path)));
        });
        options.get("ocrParallelism", String.valueOf(Runtime.getRuntime().availableProcessors()))
                .parse().asInteger().ifPresent(n -> this.ocrParallelism = Math.max(1, n));
        options.get("ocrFanout", "true").parse().asBoolean().ifPresent(b -> this.ocrFanout = b);
        options.valueIfPresent("ocrMinImageBytes").map(Long::parseLong).ifPresent(b -> this.ocrMinImageBytes = b);
        options.get("progressHeartbeatInterval", "60s").parse().asDuration()
                .ifPresent(d -> this.progressHeartbeatInterval = d);
        options.get("streamingSpew", "true").parse().asBoolean().ifPresent(b -> this.streamingSpew = b);
        options.get("spewQueueCapacity", "1000").parse().asInteger()
                .ifPresent(n -> this.spewQueueCapacity = Math.max(1, n));
        options.get("pstFolderFanout", "true").parse().asBoolean()
                .ifPresent(b -> this.pstFolderFanout = b);
        options.get("pstParseParallelism", String.valueOf(Runtime.getRuntime().availableProcessors()))
                .parse().asInteger().ifPresent(n -> this.pstParseParallelism = Math.max(1, n));
        options.get("legacyUntitledNaming", "false").parse().asBoolean()
                .ifPresent(b -> this.legacyUntitledNaming = b);
        options.get("maxEmbedDepth", String.valueOf(EmbedSpawner.DEFAULT_MAX_EMBED_DEPTH))
                .parse().asInteger().ifPresent(n -> this.maxEmbedDepth = Math.max(0, n));
        // Legacy naming and PST folder fan-out are mutually exclusive. The legacy global untitled_N
        // counter is serial-only: each fan-out fork carries its own counter starting at 0, so with
        // fan-out on the nameless embeds would get nondeterministic, non-matching names, defeating the
        // flag's backward-compat purpose. Force the serial walk so the global counter is used serially.
        if (legacyUntitledNaming && pstFolderFanout) {
            logger.warn("legacyUntitledNaming requires the serial PST walk; forcing pstFolderFanout=false.");
            pstFolderFanout = false;
        }
        logger.info("extractor configured with digester {} and {}", digester.getClass(), documentFactory);
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

    public void setOcrConfig(final OCRConfigAdapter<?> ocrConfig) {
        this.ocrConfig = ocrConfig;
        Parser ocrParser = ocrConfig.buildParser();
        replaceParser(ocrConfig.getParserClass(), parser -> ocrParser);
        // this is a hack: we are mapping TesseractOCRParser.class to Tess4jOCRParser instance
        for (OCRConfigRegistry c: OCRConfigRegistry.values()) {
            replaceParser(c.buildAdapter().getParserClass(), parser -> ocrParser);
        }
        // Route images ImageIO can decode but Tesseract cannot OCR directly (e.g. JBIG2 scans)
        // through OCR by transcoding them to PNG. excludeParser keeps this idempotent across re-config.
        excludeParser(ImageIOTranscodingOCRParser.class);
        addParser(new ImageIOTranscodingOCRParser(ocrParser));
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

    public int getOcrParallelism() { return ocrParallelism; }
    public boolean isOcrFanout() { return ocrFanout; }
    public long getOcrMinImageBytes() { return ocrMinImageBytes; }
    public boolean isStreamingSpew() { return streamingSpew; }
    public boolean isPstFolderFanout() { return pstFolderFanout; }
    public int getPstParseParallelism() { return pstParseParallelism; }
    public boolean isLegacyUntitledNaming() { return legacyUntitledNaming; }
    public int getMaxEmbedDepth() { return maxEmbedDepth; }

    ExecutorService pstParseExecutorOrNull() { return pstParseExecutor; }

    // Lazily create the shared PST folder-walk pool on first fan-out use, mirroring ocrExecutor().
    synchronized ExecutorService pstParseExecutor() {
        if (pstParseExecutor == null) {
            pstParseExecutor = Executors.newFixedThreadPool(pstParseParallelism, new ThreadFactory() {
                private final java.util.concurrent.atomic.AtomicInteger n =
                        new java.util.concurrent.atomic.AtomicInteger();
                @Override public Thread newThread(final Runnable r) {
                    final Thread t = new Thread(r, "extract-pst-walk-" + n.incrementAndGet());
                    t.setDaemon(true);
                    return t;
                }
            });
        }
        return pstParseExecutor;
    }

    /**
     * Returns the shared OCR executor, lazily CREATING it on the first call; callers that only
     * want to inspect whether a pool exists should use {@link #ocrExecutorOrNull()} instead.
     */
    public ExecutorService getOcrExecutor() { return ocrExecutor(); }

    /**
     * Returns the OCR executor field WITHOUT creating it.
     * Returns {@code null} if the pool has never been needed (no deferred OCR occurred).
     * Intended for tests that verify the lazy-creation contract.
     */
    ExecutorService ocrExecutorOrNull() { return ocrExecutor; }

    /**
     * Lazy double-checked creation of the shared OCR thread pool.
     * The pool is built at most once and only when an eligible image embed is actually deferred.
     */
    private synchronized ExecutorService ocrExecutor() {
        if (ocrExecutor == null) {
            ocrExecutor = Executors.newFixedThreadPool(ocrParallelism, new ThreadFactory() {
                private final AtomicInteger counter = new AtomicInteger();
                @Override public Thread newThread(final Runnable r) {
                    final Thread thread = new Thread(r, "extract-ocr-" + counter.incrementAndGet());
                    thread.setDaemon(true);
                    return thread;
                }
            });
        }
        return ocrExecutor;
    }

    public ExtractionProgressTracker getProgressTracker() { return progressTracker; }
    public void addProgressListener(final ProgressListener listener) {
        progressTracker.addListener(listener);
    }

    /**
     * Releases resources held by this extractor: the OCR thread pool (if it was created) and
     * the heartbeat scheduler (if it was started). Safe to call before any extraction has
     * occurred (both are null/not-started in that case) and safe to call multiple times
     * (idempotent: shutdownNow on an already-terminated pool is a no-op).
     */
    @Override
    public void close() {
        if (ocrExecutor != null) { ocrExecutor.shutdownNow(); ocrExecutor = null; }
        if (pstParseExecutor != null) { pstParseExecutor.shutdownNow(); pstParseExecutor = null; }
        if (progressTracker != null) { progressTracker.close(); }
        parseExecutor.shutdownNow();
    }

    public long getEmbedMemoryBudgetBytes() {
        return embedMemoryBudgetBytes;
    }

    public void setEmbedMemoryBudgetBytes(final long embedMemoryBudgetBytes) {
        this.embedMemoryBudgetBytes = embedMemoryBudgetBytes;
    }

    public double getEmbedMemoryPressureThreshold() {
        return embedMemoryPressureThreshold;
    }

    public void setEmbedMemoryPressureThreshold(final double embedMemoryPressureThreshold) {
        this.embedMemoryPressureThreshold = embedMemoryPressureThreshold;
    }

    /**
     * Set the wall-clock timeout for a single document's parse and output.
     * A zero or negative duration disables the watchdog.
     *
     * @param parseTimeout the timeout duration
     */
    public void setParseTimeout(final Duration parseTimeout) {
        this.parseTimeout = parseTimeout;
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

    /**
     * Set the OCR strategy used for PDFs, mapping to Tika's {@link PDFParserConfig.OCR_STRATEGY}.
     *
     * <p>{@code NO_OCR} (the default) keeps the per-inline-image OCR path
     * ({@code extractInlineImages = true}). Any rendering strategy
     * ({@code AUTO}, {@code OCR_AND_TEXT_EXTRACTION}, {@code OCR_ONLY}) renders whole pages and
     * therefore disables inline-image extraction, so a scanned page is OCR'd once as a composite
     * rather than once per embedded layer. An unknown value falls back to {@code NO_OCR}.
     *
     * <p>Has no effect once OCR has been disabled via {@link #disableOcr()}: with no OCR parser
     * left to run, a rendering strategy would only make Tika render pages and drop their text, so
     * the strategy is forced to {@code NO_OCR} to keep {@code pdfConfig} coherent with the
     * excluded parsers.
     *
     * @param strategy the strategy name, case-insensitive
     */
    public void setOcrStrategy(final String strategy) {
        PDFParserConfig.OCR_STRATEGY parsed;
        try {
            parsed = PDFParserConfig.OCR_STRATEGY.valueOf(strategy.trim().toUpperCase(Locale.ROOT));
        } catch (final IllegalArgumentException | NullPointerException e) {
            logger.warn("unknown ocrStrategy \"{}\"; falling back to NO_OCR", strategy);
            parsed = PDFParserConfig.OCR_STRATEGY.NO_OCR;
        }
        // A rendering strategy needs an OCR parser to do anything useful; if OCR is disabled it
        // would only strip text, so force NO_OCR and leave inline-image extraction off.
        if (ocrDisabled) {
            parsed = PDFParserConfig.OCR_STRATEGY.NO_OCR;
        }
        pdfConfig.setOcrStrategy(parsed);
        pdfConfig.setExtractInlineImages(!ocrDisabled && parsed == PDFParserConfig.OCR_STRATEGY.NO_OCR);
    }

    /**
     * @return the OCR strategy currently configured for PDFs
     */
    public PDFParserConfig.OCR_STRATEGY getOcrStrategy() {
        return pdfConfig.getOcrStrategy();
    }

    /**
     * @return whether inline images are extracted from PDFs for the current strategy
     */
    public boolean isExtractInlineImages() {
        return pdfConfig.isExtractInlineImages();
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
            excludeParser(OCRParserAdapter.class);
            excludeParser(ImageIOTranscodingOCRParser.class);
            ocrDisabled = true;
            pdfConfig.setExtractInlineImages(false);
            // Drop any rendering strategy: with the OCR parsers excluded it would only make Tika
            // render pages and discard their text, silently losing content on born-digital PDFs.
            pdfConfig.setOcrStrategy(PDFParserConfig.OCR_STRATEGY.NO_OCR);
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
        if (parseTimeout == null || parseTimeout.isZero() || parseTimeout.isNegative()) {
            doExtract(path, spewer);
            return;
        }

        final Future<?> future = parseExecutor.submit(() -> {
            doExtract(path, spewer);
            return null;
        });

        try {
            future.get(parseTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (final TimeoutException e) {
            // Interrupt the worker: it unblocks any pipe read, Spewer.write's finally closes the
            // reader, and the background parse thread stops on its next write. A parser in a tight
            // CPU loop that never touches the pipe cannot be killed and leaks until the next restart.
            future.cancel(true);
            throw new ParseTimeoutException(path, parseTimeout);
        } catch (final ExecutionException e) {
            // Propagate the worker's original throwable verbatim so the existing recoverable-vs-fatal
            // classification (and the fatal -> process-exit path) is unchanged.
            sneakyThrow(e.getCause());
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            future.cancel(true);
            throw new IOException("Extraction interrupted: " + path, e);
        }
    }

    private void doExtract(final Path path, final Spewer spewer) throws IOException {
        long before = currentTimeMillis();
        progressTracker.begin(path);
        try {
            if (streamingSpew && EmbedHandling.SPAWN == embedHandling) {
                try (StreamingSpewCoordinator coordinator = new StreamingSpewCoordinator(spewer, spewQueueCapacity)) {
                    // Start the spew worker BEFORE extract(): extract() constructs the pull-parser and
                    // blocks on Tika's first-character read of the ROOT pipe, which for a PST/OST never
                    // emits root text until the parse ends. The parse meanwhile produces embeds onto the
                    // bounded queue, so the worker must already be draining or the queue fills and the
                    // parse deadlocks (the first-char read can never complete). The worker drains embeds
                    // throughout the parse; spew() below writes the root last and awaits the worker.
                    coordinator.start();
                    final TikaDocument document = extract(path, coordinator);
                    logger.info("{} streaming-spew started in {}ms", path, currentTimeMillis() - before);
                    // Foreground writes the root (driving the rest of the parse), then the coordinator
                    // awaits every embed and closes the root reader (temp cleanup). start() is idempotent.
                    coordinator.spew(document);
                }
            } else {
                final TikaDocument document = extract(path);
                logger.info("{} extracted in {}ms", path, currentTimeMillis() - before);
                spewer.write(document);
            }
        } finally {
            progressTracker.end(path);
        }
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
        Throwable fatal = null;

        try {
            extract(path, spewer);
        } catch (final Throwable t) {
            if (t instanceof Exception) {
                exception = (Exception) t;
                status = status(exception, spewer);
                log(exception, status, path);
            } else {
                // An Error escaped a parser (e.g. StackOverflowError, OutOfMemoryError).
                status = ExtractionStatus.FAILURE_FATAL;
                exception = ExtractionErrors.asException(t);
                log(exception, status, path);
                if (ExtractionErrors.isFatal(t)) {
                    fatal = t;
                }
            }
        }

        // For tagged IO exceptions, discard the tag, which is either unwanted or not serializable.
        if ((exception instanceof TaggedIOException)) {
            exception = ((TaggedIOException) exception).getCause();
        }

        // Record best-effort. A fatal error may leave the JVM unable to record; never let that mask the original.
        try {
            reporter.save(path, status, exception);
        } catch (final Throwable recordingFailure) {
            if (fatal == null) {
                throw recordingFailure;
            }
            logger.error(String.format("Failed to record status for \"%s\".", path), recordingFailure);
        }

        if (fatal != null) {
            // Propagate so the worker thread's uncaught-exception handler can exit the process for a clean restart.
            sneakyThrow(fatal);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void sneakyThrow(final Throwable t) throws T {
        throw (T) t;
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
            case FAILURE_FATAL:
                logger.error(String.format("A parser threw a fatal error: \"%s\".", file), e);
                break;
            case FAILURE_TIMEOUT:
                logger.error(String.format("Parse exceeded timeout: \"%s\".", file), e);
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
        if (e instanceof ParseTimeoutException) {
            return ExtractionStatus.FAILURE_TIMEOUT;
        }

        // A transient interrupt (e.g. the worker thread being shut down) is not the file's fault;
        // do not record it as a hard "unreadable file" error.
        if (e instanceof InterruptedIOException || e.getCause() instanceof InterruptedException) {
            return ExtractionStatus.FAILURE_UNKNOWN;
        }

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
        return extract(path, (SpewSink) null);
    }

    public TikaDocument extract(final Path path, final SpewSink sink) throws IOException {
        final Function<Writer, ContentHandler> handler;
        if (OutputFormat.HTML == outputFormat) {
            handler = (writer) -> new ExpandedTitleContentHandler(new HTML5Serializer(writer));
        } else {
            handler = BodyContentHandler::new;
        }
        return getTikaDocument(path, handler, metadata -> true, sink);
    }

    public PageIndices extractPageIndices(final Path path, DocumentSelector documentSelector, String docId) throws IOException {
        Path cachedDirectory = ofNullable(embedOutput).map(p -> getEmbeddedPath(p, docId)).orElse(null);
        if (cachedDirectory != null && cachedDirectory.resolve(PAGES_JSON).toFile().exists()) {
            return new ObjectMapper().readValue(cachedDirectory.resolve(PAGES_JSON).toFile(),  PageIndices.class);
        } else {
            PageIndices pageIndices = extractPageIndices(path, documentSelector);
            if (cachedDirectory != null) {
                Files.createDirectories(cachedDirectory);
                new ObjectMapper().writeValue(cachedDirectory.resolve(PAGES_JSON).toFile(), pageIndices);
            }
            return pageIndices;
        }
    }

    public PageIndices extractPageIndices(final Path path) throws IOException {
        return extractPageIndices(path, metadata -> true);
    }

    public List<String> extractPages(final Path path) throws IOException {
        return extractPages(path, metadata -> true);
    }

    /**
     * the model List<String>> works for one document. If we wanted to do it for a document tree it would need
     * a composite pattern with a representation like:
     * DocumentPages = [ [Text, [[Text], [Text ]], Text ],  [Text]]
     *
     * (for a doc that has two pages with an embedded  2 pages doc in the middle of its first page)
     *
     * @param path
     * @param documentSelector
     * @return
     * @throws IOException
     */
    public List<String> extractPages(Path path, DocumentSelector documentSelector) throws IOException {
        PagesContentHandler contentHandler = createContentHandlerForPages();
        final Function<Writer, ContentHandler> handlerProvider = (writer) -> contentHandler;
        TikaDocument tikaDocument = getTikaDocument(path, handlerProvider, documentSelector);
        try (final Reader reader = tikaDocument.getReader()) {
            Spewer.copy(reader, Writer.nullWriter());
        }
        return contentHandler.getPages();
    }

    /**
     * Same note as the page extraction : the List<Pair<Long, Long>> works with a single doc.
     * In the same use case as previous method we'd have:
     *
     * DocumentPageIndices = [ [(i1, i2), [[(i3, i4)], [(i5, i6) ]], (i7, i8) ],  [(i9, i10)]]
     *
     * @param path
     * @param documentSelector
     * @return
     * @throws IOException
     */
    public PageIndices extractPageIndices(final Path path, DocumentSelector documentSelector) throws IOException {
        PageIndicesContentHandler contentHandler = createContentHandlerForPageIndices();
        final Function<Writer, ContentHandler> handlerProvider = (writer) -> contentHandler;
        TikaDocument tikaDocument = getTikaDocument(path, handlerProvider, documentSelector);
        try (final Reader reader = tikaDocument.getReader()) {
            Spewer.copy(reader, Writer.nullWriter());
        }
        return contentHandler.getPageIndices();
    }

    private PagesContentHandler createContentHandlerForPages() {
        if (OutputFormat.HTML == outputFormat) {
            return new PagesContentHandler(new ExpandedTitleContentHandler(new HTML5Serializer(Writer.nullWriter())));
        } else {
            return new PagesContentHandler(new BodyContentHandler(Writer.nullWriter()));
        }
    }

    private PageIndicesContentHandler createContentHandlerForPageIndices() {
        if (OutputFormat.HTML == outputFormat) {
            return new PageIndicesContentHandler(new ExpandedTitleContentHandler(new HTML5Serializer(Writer.nullWriter())));
        } else {
            return new PageIndicesContentHandler(new BodyContentHandler(Writer.nullWriter()));
        }
    }

    private TikaDocument getTikaDocument(Path path, final Function<Writer, ContentHandler> handlerProvider, DocumentSelector documentSelector) throws IOException {
        return getTikaDocument(path, handlerProvider, documentSelector, null);
    }

    private TikaDocument getTikaDocument(Path path, final Function<Writer, ContentHandler> handlerProvider, DocumentSelector documentSelector, final SpewSink sink) throws IOException {
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
            context.set(TesseractOCRConfig.class, ocrConfig.getConfig());
        }

        context.set(PDFParserConfig.class, pdfConfig);
        context.set(DocumentSelector.class, documentSelector);

        // Only include "safe" tags in the HTML output from Tika's HTML parser.
        // This excludes script tags and objects.
        context.set(HtmlMapper.class, DefaultHtmlMapper.INSTANCE);

        TemporaryResources embedTextResources = null;
        if (EmbedHandling.SPAWN == embedHandling) {
            context.set(Parser.class, parser);
            embedTextResources = new TemporaryResources();
            // Live progress for this path (null when called outside doExtract, e.g. page extraction).
            final ExtractionProgress currentProgress = progressTracker.get(path);
            // Class name of the configured OCR parser, set SYNCHRONOUSLY on the shared embed metadata
            // by the deferred-OCR path so the indexed OCR_PARSER matches what serial mode produces
            // (OCRParserAdapter sets OCR_PARSER to its delegate parser's class name, i.e. the
            // configured OCR parser class). Datashare's on-demand SourceExtractor.useOcr() reads this.
            final String ocrParserClassName =
                    (ocrDisabled || ocrConfig == null) ? null : ocrConfig.getParserClass().getName();
            // Pass the lazy supplier so the OCR pool is created only when an eligible image
            // is actually deferred, not at Extractor construction time.
            context.set(EmbeddedDocumentExtractor.class,
                    new EmbedSpawner(rootDocument, context, embedOutput, handlerProvider, embedMemoryBudgetBytes,
                            embedTextResources, new MemoryPressureGauge(embedMemoryPressureThreshold),
                            this::ocrExecutor, !ocrDisabled, currentProgress, digester,
                            ocrFanout, ocrMinImageBytes, ocrParserClassName, sink, legacyUntitledNaming,
                            maxEmbedDepth));
            context.set(org.icij.extract.parser.PstFanoutConfig.class,
                    new org.icij.extract.parser.PstFanoutConfig(pstFolderFanout, this::pstParseExecutor));
        } else if (EmbedHandling.CONCATENATE == embedHandling) {
            context.set(Parser.class, parser);
            context.set(EmbeddedDocumentExtractor.class, new EmbedParser(rootDocument, context));
        } else {
            context.set(Parser.class, EmptyParser.INSTANCE);
            context.set(EmbeddedDocumentExtractor.class, new EmbedBlocker());
        }

        try {
            Reader reader = new ParsingReaderWithContentHandler(parser, tikaInputStream, rootDocument.getMetadata(), context, handlerProvider);
            if (null != embedTextResources) {
                // Delete spilled embed-text temp files when the root reader is closed.
                reader = new ResourceClosingReader(reader, embedTextResources);
            }
            rootDocument.setReader(reader);
            return rootDocument;
        } catch (final Exception e) {
            if (null != embedTextResources) {
                try {
                    embedTextResources.close();
                } catch (final IOException suppressed) {
                    e.addSuppressed(suppressed);
                }
            }
            throw e;
        }
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

    public static CompositeParser addParser(final Parser parser, final Parser toAdd) {
        if (parser instanceof CompositeParser composite) {
            final List<Parser> parsers = new ArrayList<>();
            getAllSubParsers(composite).forEach(parsers::add);
            // Append last so it overrides existing mappings for its media types in getParsers().
            parsers.add(toAdd);
            return new CompositeParser(composite.getMediaTypeRegistry(), parsers);
        }
        return null;
    }

    private void addParser(final Parser toAdd) {
        defaultParser = addParser(defaultParser, toAdd);
    }

    private void replaceParser(final Class<? extends Parser> exclude, final Function<Parser, Parser> parserFn) {
        defaultParser = replaceParser(defaultParser, exclude, parserFn);
    }
}
