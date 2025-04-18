package org.icij.extract.ocr;

import static com.sun.jna.Platform.isMac;
import static com.sun.jna.Platform.isWindows;
import static net.sourceforge.tess4j.util.LoadLibs.getTesseractLibName;
import static org.apache.tika.metadata.TikaCoreProperties.CONTENT_TYPE_PARSER_OVERRIDE;
import static org.icij.extract.LambdaExceptionUtils.rethrowFunction;

import com.recognition.software.jdeskew.ImageUtil;
import com.sun.jna.Library;
import com.sun.jna.NativeLibrary;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.imageio.ImageIO;
import net.sourceforge.tess4j.ITessAPI;
import net.sourceforge.tess4j.ITesseract;
import net.sourceforge.tess4j.OCRResult;
import net.sourceforge.tess4j.OSDResult;
import net.sourceforge.tess4j.TessAPI;
import net.sourceforge.tess4j.Tesseract;
import net.sourceforge.tess4j.TesseractException;
import net.sourceforge.tess4j.util.ImageHelper;
import org.apache.commons.io.FilenameUtils;
import org.apache.tika.config.Field;
import org.apache.tika.config.TikaTaskTimeout;
import org.apache.tika.exception.TikaConfigException;
import org.apache.tika.exception.TikaException;
import org.apache.tika.extractor.ParentContentHandler;
import org.apache.tika.io.TemporaryResources;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.Property;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.mime.MediaType;
import org.apache.tika.mime.MimeType;
import org.apache.tika.mime.MimeTypeException;
import org.apache.tika.mime.MimeTypes;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.ocr.TesseractOCRConfig;
import org.apache.tika.parser.ocr.tess4j.ImageDeskew;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.EmbeddedContentHandler;
import org.apache.tika.sax.TeeContentHandler;
import org.apache.tika.sax.XHTMLContentHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;


public class Tess4JOCRParser extends ParserWithConfidence implements Parser, AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(Tess4JOCRParser.class);
    private static final String JNA_LIBRARY_PATH = "jna.library.path";

    private final TesseractOCRConfig defaultConfig = new TesseractOCRConfig();
    final ExecutorService executor = Executors.newSingleThreadExecutor();

    private String tessdataPath = "";
    private String tesseractPath = "";

    static {
        // TODO: make this more more generic, hardcoding the homebrew path is ugly.
        //  Additionally this might lack robustness, the TessAPI is a singleton, if anyone loads it before reaching this
        //  piece it will be loaded from the wrong path. Sadly Tess4J doesn't seem to let us override this behavior
        //  easily... So we're leaving it as is until we get bitten
        if (isMac()) {
            Path homebrewTessPath = Paths.get("/opt/homebrew/lib/libtesseract.dylib");
            if (homebrewTessPath.toFile().exists()) {
                String macJNAPath = homebrewTessPath.getParent().toString();
                macJNAPath += Optional.ofNullable(System.getProperty(JNA_LIBRARY_PATH))
                    .map(p -> File.pathSeparator + p).orElse("");
                System.setProperty(JNA_LIBRARY_PATH, macJNAPath);
            }
        }
    }


    private static final Set<MediaType> SUPPORTED_TYPES = Set.of(
        MediaType.image("ocr-png"),
        MediaType.image("ocr-jpeg"),
        MediaType.image("ocr-tiff"),
        MediaType.image("ocr-bmp"),
        MediaType.image("ocr-gif"),
        MediaType.image("jp2"),
        MediaType.image("jpx"),
        MediaType.image("x-portable-pixmap"),
        MediaType.image("ocr-jp2"),
        MediaType.image("ocr-jpx"),
        MediaType.image("ocr-x-portable-pixmap")
    );

    public static final Property IMAGE_ROTATION = Property.externalRealSeq("tess:rotation");
    public static final Property PSM0_ROTATE = Property.externalInteger("tess:rotate");
    public static final Property PSM0_ORIENTATION = Property.externalInteger("tess:orientation");
    public static final Property PSM0_ORIENTATION_CONFIDENCE = Property.externalReal("tess:orientation_confidence");
    public static final Property PSM0_SCRIPT = Property.externalText("tess:script");
    public static final Property PSM0_SCRIPT_CONFIDENCE = Property.externalReal("tess:script_confidence");

    public static final String SKIP_CONFIDENCE = "skipConfidence";


    private volatile ITesseract tesseract;
    private List<ITesseract.RenderedFormat> supportedFormats;

    public String getTesseractPath() {
        return this.tesseractPath;
    }

    @Field
    public void setTesseractPath(String tesseractPath) {
        tesseractPath = FilenameUtils.normalize(tesseractPath);
        if (!tesseractPath.isEmpty() && !tesseractPath.endsWith(File.separator)) {
            tesseractPath = tesseractPath + File.separator;
        }

        this.tesseractPath = tesseractPath;
    }

    public String getTessdataPath() {
        return this.tessdataPath;
    }

    @Field
    public void setTessdataPath(String tessdataPath) {
        tessdataPath = FilenameUtils.normalize(tessdataPath);
        if (!tessdataPath.isEmpty() && !tessdataPath.endsWith(File.separator)) {
            tessdataPath = tessdataPath + File.separator;
        }

        this.tessdataPath = tessdataPath;
    }

    @Field
    public void setOtherTesseractSettings(List<String> settings) throws TikaConfigException {
        for (String s : settings) {
            String[] bits = s.trim().split("\\s+");
            if (bits.length != 2) {
                throw new TikaConfigException(
                    "Expected space delimited key value pair. However, I found " + bits.length + " bits.");
            }
            this.defaultConfig.addOtherTesseractConfig(bits[0], bits[1]);
        }

    }

    public List<String> getOtherTesseractSettings() {
        Map<String, String> sorted = new TreeMap<>(this.defaultConfig.getOtherTesseractConfig());
        return sorted.entrySet().stream().map((e) -> e.getKey() + " " + e.getValue()).toList();
    }

    @Field
    public void setSkipOCR(boolean skipOCR) {
        this.defaultConfig.setSkipOcr(skipOCR);
    }

    public boolean isSkipOCR() {
        return this.defaultConfig.isSkipOcr();
    }

    @Field
    public void setLanguage(String language) {
        this.defaultConfig.setLanguage(language);
    }

    public String getLanguage() {
        return this.defaultConfig.getLanguage();
    }

    @Field
    public void setPageSegMode(String pageSegMode) {
        this.defaultConfig.setPageSegMode(pageSegMode);
    }

    public String getPageSegMode() {
        return this.defaultConfig.getPageSegMode();
    }

    @Field
    public void setMaxFileSizeToOcr(long maxFileSizeToOcr) {
        this.defaultConfig.setMaxFileSizeToOcr(maxFileSizeToOcr);
    }

    public long getMaxFileSizeToOcr() {
        return this.defaultConfig.getMaxFileSizeToOcr();
    }

    @Field
    public void setMinFileSizeToOcr(long minFileSizeToOcr) {
        this.defaultConfig.setMinFileSizeToOcr(minFileSizeToOcr);
    }

    public long getMinFileSizeToOcr() {
        return this.defaultConfig.getMinFileSizeToOcr();
    }

    @Field
    public void setTimeout(int timeout) {
        this.defaultConfig.setTimeoutSeconds(timeout);
    }

    public int getTimeout() {
        return this.defaultConfig.getTimeoutSeconds();
    }

    @Field
    public void setOutputType(String outputType) {
        this.defaultConfig.setOutputType(outputType);
    }

    public String getOutputType() {
        return this.defaultConfig.getOutputType().name();
    }

    @Field
    public void setPreserveInterwordSpacing(boolean preserveInterwordSpacing) {
        this.defaultConfig.setPreserveInterwordSpacing(preserveInterwordSpacing);
    }

    public boolean isPreserveInterwordSpacing() {
        return this.defaultConfig.isPreserveInterwordSpacing();
    }

    @Field
    public void setEnableImagePreprocessing(boolean enableImagePreprocessing) {
        this.defaultConfig.setEnableImagePreprocessing(enableImagePreprocessing);
    }

    public boolean isEnableImagePreprocessing() {
        return this.defaultConfig.isEnableImagePreprocessing();
    }

    @Field
    public void setDensity(int density) {
        this.defaultConfig.setDensity(density);
    }

    public int getDensity() {
        return this.defaultConfig.getDensity();
    }

    @Field
    public void setDepth(int depth) {
        this.defaultConfig.setDepth(depth);
    }

    public int getDepth() {
        return this.defaultConfig.getDepth();
    }

    @Field
    public void setColorspace(String colorspace) {
        this.defaultConfig.setColorspace(colorspace);
    }

    public String getColorspace() {
        return this.defaultConfig.getColorspace();
    }

    @Field
    public void setFilter(String filter) {
        this.defaultConfig.setFilter(filter);
    }

    public String getFilter() {
        return this.defaultConfig.getFilter();
    }

    @Field
    public void setResize(int resize) {
        this.defaultConfig.setResize(resize);
    }

    public int getResize() {
        return this.defaultConfig.getResize();
    }

    @Field
    public void setApplyRotation(boolean applyRotation) {
        this.defaultConfig.setApplyRotation(applyRotation);
    }

    public boolean isApplyRotation() {
        return this.defaultConfig.isApplyRotation();
    }

    @Field
    public void setInlineContent(boolean inlineContent) {
        this.defaultConfig.setInlineContent(inlineContent);
    }

    public boolean isInlineContent() {
        return this.defaultConfig.isInlineContent();
    }

    @Override
    public Set<MediaType> getSupportedTypes(ParseContext context) {
        return SUPPORTED_TYPES;
    }

    @Field
    public void setSkipConfidence(boolean skipConfidence) throws TikaConfigException {
        setOtherTesseractSettings(List.of(SKIP_CONFIDENCE + " " + skipConfidence));
    }

    public boolean getSkipConfidence() {
        return Boolean.parseBoolean(this.defaultConfig.getOtherTesseractConfig().get(SKIP_CONFIDENCE));
    }


    private ITesseract getOrInit(TesseractOCRConfig config) throws TikaConfigException {
        if (tesseract == null) {
            synchronized (this) {
                if (tesseract == null) {
                    tesseract = new Tesseract();
                    if (!getTessdataPath().isEmpty()) {
                        // Ugly hack, we have to reload the lib... Sadly Tess4J doesn't let us override the lib
                        // loading easily and it doesn't let
                        Library.Handler handler = new Library.Handler(getTesseractLibName(), TessAPI.class, Map.of());
                        NativeLibrary nativeLibrary = handler.getNativeLibrary();
                        String dataPath = nativeLibrary.getFile().getPath();
                        tesseract.setDatapath(dataPath);
                    } else {
                        tesseract.setDatapath(getTessdataPath());
                    }
                    tesseract.setVariable("debug_file", (isWindows()) ? "NUL" : "/dev/null");
                    tesseract.setLanguage(config.getLanguage());
                    tesseract.setPageSegMode(Integer.parseInt(config.getPageSegMode()));
                    tesseract.setVariable("page_separator", config.getPageSeparator());
                    if (config.isPreserveInterwordSpacing()) {
                        tesseract.setVariable("preserve_interword_spaces", "1");
                    } else {
                        tesseract.setVariable("preserve_interword_spaces", "0");
                    }
                    supportedFormats = List.of(Objects.requireNonNull(getRenderedFormat(config)));
                }
            }
        }
        return tesseract;
    }

    public void parse(InputStream stream, ContentHandler handler, Metadata metadata, ParseContext parseContext)
        throws IOException, SAXException, TikaException, UnsatisfiedLinkError {
        TesseractOCRConfig userConfig = parseContext.get(TesseractOCRConfig.class);
        TesseractOCRConfig config = (userConfig != null) ? defaultConfig.cloneAndUpdate(userConfig) : defaultConfig;
        if (!config.isSkipOcr()) {
            try (TemporaryResources tmp = new TemporaryResources()) {
                TikaInputStream tikaStream = TikaInputStream.get(stream, tmp, metadata);
                tikaStream.getPath();
                long size = tikaStream.getLength();
                if (size >= config.getMinFileSizeToOcr() && size <= config.getMaxFileSizeToOcr()) {
                    ContentHandler baseHandler = this.getContentHandler(
                        config.isInlineContent(), handler, metadata, parseContext
                    );
                    XHTMLContentHandler xhtml = new XHTMLContentHandler(baseHandler, metadata);
                    xhtml.startDocument();
                    parse(tikaStream, xhtml, config, parseContext, metadata, tmp);
                    xhtml.endDocument();
                }
            }
        }
    }

    private void parse(
        TikaInputStream stream, XHTMLContentHandler xhtml, TesseractOCRConfig config, ParseContext parseContext,
        Metadata metadata, TemporaryResources tmp
    ) throws TikaException, IOException, UnsatisfiedLinkError {
        File processedImageFile = null;
        try {
            tesseract = getOrInit(config);
            BufferedImage bufferedImage = ImageIO.read(stream);
            processedImageFile = preprocessImage(stream, bufferedImage, config, metadata);
            Callable<?> ocrRunner = buildOCRRunner(
                bufferedImage, stream.getFile(), processedImageFile, config, metadata, tmp, xhtml
            );
            int timeoutMillis = (int) TikaTaskTimeout.getTimeoutMillis(
                parseContext, config.getTimeoutSeconds() * 1000L
            );
            runWithTimeout(ocrRunner, timeoutMillis);
        } catch (IOException | TikaException | UnsatisfiedLinkError e) {
            logger.error(e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
        } finally {
            if (processedImageFile != null && processedImageFile.exists()) {
                boolean ignored = processedImageFile.delete();
            }
        }
    }

    private File preprocessImage(TikaInputStream stream, BufferedImage bufferedImage, TesseractOCRConfig config, Metadata metadata) throws MimeTypeException, IOException {
        // Trick tika to get a properly named file
        MimeTypes mimeTypes = MimeTypes.getDefaultMimeTypes();
        String imageExt = Optional.ofNullable(metadata.get(CONTENT_TYPE_PARSER_OVERRIDE))
            .map(mime -> mime.replace("ocr-", ""))
            .map(rethrowFunction(mimeTypes::forName))
            .map(MimeType::getExtension)
            .orElse("");
        Path tmpImPath = stream.getPath();
        String imFileName = tmpImPath.getFileName().toString();
        Path pathWithExt = tmpImPath.resolveSibling(imFileName.substring(0, imFileName.lastIndexOf('.')) + imageExt);
        // TIFF might be multipage, they are only properly handled by Tess4j as file and not as streams
        boolean isTiff = Arrays.stream(
                Optional.ofNullable(metadata.getValues(CONTENT_TYPE_PARSER_OVERRIDE)).orElse(new String[] {}))
            .anyMatch(c -> c.endsWith("tiff"));
        File processedImageFile = null;
        if (config.isEnableImagePreprocessing() || config.isApplyRotation()) {
            bufferedImage = processImage(bufferedImage, config, metadata);
            processedImageFile = pathWithExt.toFile();
            try (FileOutputStream os = new FileOutputStream(processedImageFile)) {
                boolean foundWriter = ImageIO.write(bufferedImage, imageExt.substring(1), os);
                if (!foundWriter) {
                    String msg = "Couldn't find writer for " + imageExt + " extension";
                    throw new RuntimeException(msg);
                }
            }
        } else if (isTiff) {
            processedImageFile = pathWithExt.toFile();
            Files.createSymbolicLink(pathWithExt, tmpImPath);
        }
        return processedImageFile;
    }

    private Callable<?> buildOCRRunner(
        BufferedImage bufferedImage, File rawImageFile, File processedImageFile, TesseractOCRConfig config,
        Metadata metadata, TemporaryResources tmp, XHTMLContentHandler xhtml
    ) throws IOException {
        if (config.getPageSegMode().equals("0")) {
            return new OSDRunner(bufferedImage, processedImageFile, metadata);
        }
        if (getSkipConfidence() && config.getOutputType().equals(TesseractOCRConfig.OUTPUT_TYPE.TXT)) {
            return new TextOCRRunner(bufferedImage, processedImageFile, xhtml);
        }
        if (processedImageFile != null) {
            return new OCRRunner(null, processedImageFile, config, metadata, xhtml, tmp);
        }
        return new OCRRunner(bufferedImage, rawImageFile, config, metadata, xhtml, tmp);
    }

    private static void extractOSDOutput(OSDResult osd, Metadata metadata) {
        Optional.of(osd.getOrientDeg()).ifPresent(o -> metadata.set(PSM0_ORIENTATION, o));
        Optional.of(osd.getOrientConf()).ifPresent(c -> metadata.set(PSM0_ORIENTATION_CONFIDENCE, c));
        Optional.of(osd.getOrientDeg()).ifPresent(o -> metadata.set(PSM0_ROTATE, -o));
        Optional.of(osd.getScriptName()).ifPresent(c -> metadata.set(PSM0_SCRIPT, c));
        Optional.of(osd.getScriptConf()).ifPresent(c -> metadata.set(PSM0_SCRIPT_CONFIDENCE, c));
    }

    // TODO: if we can rely on imagick density, depth, filter
    private BufferedImage processImage(BufferedImage bufferedImage, TesseractOCRConfig config, Metadata metadata) {
        ImageDeskew id = new ImageDeskew(bufferedImage);
        double imageSkewAngle = id.getSkewAngle();
        final double MINIMUM_DESKEW_THRESHOLD = 0.05d;
        if (config.isApplyRotation()) {
            if ((imageSkewAngle > MINIMUM_DESKEW_THRESHOLD || imageSkewAngle < -(MINIMUM_DESKEW_THRESHOLD))) {
                metadata.set(Tess4JOCRParser.IMAGE_ROTATION, imageSkewAngle);
                bufferedImage = ImageUtil.rotate(bufferedImage, -imageSkewAngle, 0, 0);
            }
        }
        if (config.getResize() != 100) {
            int w = (int) (0.01 * bufferedImage.getWidth() * config.getResize());
            int h = (int) (0.01 * bufferedImage.getHeight() * config.getResize());
            bufferedImage = ImageHelper.getScaledInstance(bufferedImage, w, h);
        }
        if (config.getColorspace() != null) {
            if (config.getColorspace().equals("gray")) {
                bufferedImage = ImageHelper.convertImageToGrayscale(bufferedImage);
            } else {
                throw new IllegalStateException("Unexpected colorspace value: " + config.getColorspace());
            }
        }
        return bufferedImage;
    }

    private static void extractOCROutput(InputStream stream, ContentHandler xhtml) throws SAXException, IOException {
        AttributesImpl attrs = new AttributesImpl();
        attrs.addAttribute("", "class", "class", "CDATA", "ocr");
        xhtml.startElement("http://www.w3.org/1999/xhtml", "div", "div", attrs);
        try (Reader reader = new InputStreamReader(stream, StandardCharsets.UTF_8)) {
            char[] buffer = new char[1024];
            for (int n = reader.read(buffer); n != -1; n = reader.read(buffer)) {
                if (n > 0) {
                    xhtml.characters(buffer, 0, n);
                }
            }
        }
        xhtml.endElement("http://www.w3.org/1999/xhtml", "div", "div");
    }

    private ContentHandler getContentHandler(
        boolean isInlineContent, ContentHandler handler, Metadata metadata, ParseContext parseContext
    ) {
        if (!isInlineContent) {
            return handler;
        } else {
            ParentContentHandler parentContentHandler = parseContext.get(ParentContentHandler.class);
            if (parentContentHandler == null) {
                return handler;
            } else {
                String embeddedType = metadata.get(TikaCoreProperties.EMBEDDED_RESOURCE_TYPE);
                return !TikaCoreProperties.EmbeddedResourceType.INLINE.name().equals(embeddedType) ? handler :
                    new TeeContentHandler(
                        new EmbeddedContentHandler(new BodyContentHandler(parentContentHandler.getContentHandler())),
                        handler
                    );
            }
        }
    }

    private static ITesseract.RenderedFormat getRenderedFormat(TesseractOCRConfig config) {
        switch (config.getOutputType()) {
            case TXT -> {
                return ITesseract.RenderedFormat.TEXT;
            }
            case HOCR -> {
                return ITesseract.RenderedFormat.HOCR;
            }
        }
        return null;
    }

    private class OSDRunner implements Callable<Void> {
        private final BufferedImage image;
        private final File imageFile;
        private final Metadata metadata;

        public OSDRunner(BufferedImage image, File imageFile, Metadata metadata) {
            this.image = image;
            this.imageFile = imageFile;
            this.metadata = metadata;
        }

        @Override
        public Void call() {
            OSDResult osdResult = Optional.ofNullable(imageFile)
                .map(rethrowFunction(tesseract::getOSD))
                .orElse(tesseract.getOSD(image));
            extractOSDOutput(osdResult, metadata);
            return null;
        }
    }

    private class TextOCRRunner implements Callable<Void> {
        private final BufferedImage image;
        private final File imageFile;
        private final ContentHandler xhtml;

        public TextOCRRunner(BufferedImage image, File imageFile, ContentHandler xhtml) {
            this.image = image;
            this.imageFile = imageFile;
            this.xhtml = xhtml;
        }

        @Override
        public Void call() throws TesseractException, IOException, SAXException {
            String ocrOutput = Optional.ofNullable(imageFile)
                .map(rethrowFunction(tesseract::doOCR)).orElse(tesseract.doOCR(image));
            extractOCROutput(new ByteArrayInputStream(ocrOutput.getBytes()), xhtml);
            return null;
        }

    }

    private class OCRRunner implements Callable<Void> {
        private final BufferedImage image;
        private final File imageFile;
        private final TesseractOCRConfig config;
        private final ContentHandler xhtml;
        private final Metadata metadata;
        private final TemporaryResources tmp;

        public OCRRunner(
            BufferedImage image, File imageFile, TesseractOCRConfig config, Metadata metadata, ContentHandler xhtml,
            TemporaryResources tmp
        ) {
            this.image = image;
            this.imageFile = imageFile;
            this.metadata = metadata;
            this.config = config;
            this.xhtml = xhtml;
            this.tmp = tmp;
        }

        @Override
        public Void call() throws IOException, SAXException, TesseractException {
            File outputFile = tmp.createTemporaryFile();
            try {
                OCRResult res;
                boolean isHOCR = config.getOutputType().equals(TesseractOCRConfig.OUTPUT_TYPE.HOCR);
                if (isHOCR) {
                    tesseract.setVariable("tessedit_create_hocr", "1");
                } else {
                    tesseract.setVariable("tessedit_create_hocr", "0");
                }
                if (image != null) {
                    res = tesseract.createDocumentsWithResults(
                        image,
                        imageFile.getAbsolutePath(),
                        outputFile.getAbsolutePath(),
                        supportedFormats,
                        ITessAPI.TessPageIteratorLevel.RIL_BLOCK
                    );
                } else {
                    res = tesseract.createDocumentsWithResults(
                        imageFile.getAbsolutePath(),
                        outputFile.getAbsolutePath(),
                        supportedFormats,
                        ITessAPI.TessPageIteratorLevel.RIL_BLOCK
                    );
                }
                metadata.set(OCR_CONFIDENCE, 0.01 * res.getConfidence());
                if (isHOCR) {
                    outputFile = new File(outputFile.toPath().resolveSibling(outputFile.getName() + ".hocr").toUri());
                }
                extractOCROutput(new FileInputStream(outputFile), xhtml);
            } finally {
                if (outputFile.exists()) {
                    boolean ignored = outputFile.delete();
                }
            }
            return null;
        }
    }

    private void runWithTimeout(Callable<?> callable, Integer timeoutMs) throws ExecutionException, TikaException {
        final Future<?> future = executor.submit(callable);
        try {
            future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TikaException(Tess4JOCRParser.class.getSimpleName() + " interrupted", e);
        } catch (TimeoutException e) {
            throw new TikaException(
                Tess4JOCRParser.class.getSimpleName() + " exceeded " + timeoutMs * 1000 + "ms timeout", e
            );
        }
    }

    @Override
    public void close() throws Exception {
        if (!executor.isTerminated()) {
            executor.shutdown();
        }
    }
}