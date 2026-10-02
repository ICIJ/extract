package org.icij.extract.ocr;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TemporaryResources;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.Property;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.ocr.TesseractOCRConfig;
import org.apache.tika.parser.ocr.TesseractOCRParser;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.XHTMLContentHandler;
import org.apache.tika.utils.ParserUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BooleanSupplier;

import static org.icij.extract.ocr.ParserWithConfidence.OCR_CONFIDENCE;

/**
 * Picks the tesseract model per image: detects the script with OSD, reads the image with that script's
 * model plus Latin, and reads it again with every supported script when the first read scores below
 * the retry confidence, keeping the better read.
 */
public class AutoLanguageOCRParser implements Parser {
    public static final Property OCR_SCRIPT = Property.externalText("ocr:script");
    public static final Property OCR_SCRIPT_CONFIDENCE = Property.externalReal("ocr:script_confidence");
    public static final Property OCR_MODEL = Property.externalText("ocr:model");

    private static final Logger LOGGER = LoggerFactory.getLogger(AutoLanguageOCRParser.class);
    private static final String LATIN = "script/Latin";
    private static final List<String> SCRIPTS = List.of(
            LATIN, "script/HanS", "script/Cyrillic", "script/Arabic", "script/Japanese", "script/Hangul");
    private static final String ALL_SCRIPTS = String.join("+", SCRIPTS);
    private static final String OSD_OUTPUT = ".osd";
    // OSD names Hangul text "Korean".
    private static final Map<String, String> FIRST_PASS_MODEL = Map.of(
            "Latin", LATIN,
            "Han", "script/HanS+" + LATIN,
            "Cyrillic", "script/Cyrillic+" + LATIN,
            "Arabic", "script/Arabic+" + LATIN,
            "Hangul", "script/Hangul+" + LATIN,
            "Korean", "script/Hangul+" + LATIN,
            "Japanese", "script/Japanese+" + LATIN,
            "Hiragana", "script/Japanese+" + LATIN,
            "Katakana", "script/Japanese+" + LATIN);

    private final Parser delegate;
    private final boolean readsHocr;
    private final int retryConfidence;
    private final BooleanSupplier enabled;

    public AutoLanguageOCRParser(Parser delegate, boolean readsHocr, int retryConfidence, BooleanSupplier enabled) {
        this.delegate = delegate;
        this.readsHocr = readsHocr;
        this.retryConfidence = retryConfidence;
        this.enabled = enabled;
    }

    public static Set<String> missingModels(Set<String> installed) {
        Set<String> missing = new TreeSet<>(SCRIPTS);
        missing.add("osd");
        missing.removeAll(installed);
        return missing;
    }

    @Override
    public Set<MediaType> getSupportedTypes(ParseContext context) {
        return delegate.getSupportedTypes(context);
    }

    @Override
    public void parse(InputStream stream, ContentHandler handler, Metadata metadata, ParseContext context)
            throws IOException, SAXException, TikaException {
        if (!enabled.getAsBoolean()) {
            delegate.parse(stream, handler, metadata, context);
            return;
        }
        TesseractOCRConfig callerConfig = context.get(TesseractOCRConfig.class);
        TesseractOCRConfig base = callerConfig == null ? new TesseractOCRConfig() : callerConfig;
        try (TemporaryResources tmp = new TemporaryResources()) {
            Path image = TikaInputStream.get(stream, tmp, metadata).getPath();
            if (readsAsIs(base, Files.size(image))) {
                run(image, base, handler, metadata, context);
                return;
            }
            Metadata detection = detect(image, base, metadata, context);
            String script = StringUtils.trimToNull(detection.get(TesseractOCRParser.PSM0_SCRIPT));
            String firstModel = script == null ? LATIN : FIRST_PASS_MODEL.getOrDefault(script, ALL_SCRIPTS);
            Pass best = read(image, firstModel, base, metadata, context);
            if (best.confidence() < retryConfidence && !best.text().isEmpty() && !firstModel.equals(ALL_SCRIPTS)) {
                try {
                    Pass retry = read(image, ALL_SCRIPTS, base, metadata, context);
                    if (retry.confidence() > best.confidence()) {
                        best = retry;
                    }
                } catch (IOException | SAXException | TikaException e) {
                    LOGGER.warn("retry with {} failed, keeping the first pass: {}", ALL_SCRIPTS, e.toString());
                }
            }
            emit(best.text(), handler, metadata);
            if (script != null) {
                metadata.set(OCR_SCRIPT, script);
                Optional.ofNullable(detection.get(TesseractOCRParser.PSM0_SCRIPT_CONFIDENCE))
                        .ifPresent(confidence -> metadata.set(OCR_SCRIPT_CONFIDENCE, Double.parseDouble(confidence)));
            }
            metadata.set(OCR_MODEL, best.model());
            metadata.set(OCR_CONFIDENCE, best.confidence() / 100);
        } finally {
            context.set(TesseractOCRConfig.class, callerConfig);
        }
    }

    private static boolean readsAsIs(TesseractOCRConfig config, long size) {
        return config.isSkipOcr() || "0".equals(config.getPageSegMode())
                || size < config.getMinFileSizeToOcr() || size > config.getMaxFileSizeToOcr();
    }

    private Metadata detect(Path image, TesseractOCRConfig base, Metadata metadata, ParseContext context) {
        TesseractOCRConfig config = SerializationUtils.clone(base);
        config.setPageSegMode("0");
        Metadata scratch = ParserUtils.cloneMetadata(metadata);
        try {
            run(image, config, new DefaultHandler(), scratch, context);
            return scratch;
        } catch (IOException | SAXException | TikaException e) {
            LOGGER.debug("script detection failed, reading as {}: {}", LATIN, e.toString());
            if (readsHocr) {
                deleteOrphanOsdOutput();
            }
            return new Metadata();
        }
    }

    // When tesseract fails, Tika deletes its apache-tika-*.tmp base file but not the .osd output next to it.
    private static void deleteOrphanOsdOutput() {
        Path tmp = Path.of(System.getProperty("java.io.tmpdir"));
        try (DirectoryStream<Path> outputs = Files.newDirectoryStream(tmp, "apache-tika-*.tmp" + OSD_OUTPUT)) {
            for (Path output : outputs) {
                String name = output.getFileName().toString();
                if (Files.notExists(output.resolveSibling(name.substring(0, name.length() - OSD_OUTPUT.length())))) {
                    Files.deleteIfExists(output);
                }
            }
        } catch (IOException e) {
            LOGGER.debug("could not delete tesseract OSD output: {}", e.toString());
        }
    }

    private Pass read(Path image, String model, TesseractOCRConfig base, Metadata metadata, ParseContext context)
            throws IOException, SAXException, TikaException {
        TesseractOCRConfig config = SerializationUtils.clone(base);
        config.setLanguage(model);
        metadata.remove(OCR_CONFIDENCE.getName());
        if (readsHocr) {
            config.setOutputType(TesseractOCRConfig.OUTPUT_TYPE.HOCR);
            HocrTextHandler hocr = new HocrTextHandler();
            run(image, config, hocr, metadata, context);
            return new Pass(model, asTesseractText(hocr.text()), hocr.meanConfidence());
        }
        config.addOtherTesseractConfig(Tess4JOCRParser.SKIP_CONFIDENCE, "false");
        BodyContentHandler text = new BodyContentHandler(-1);
        run(image, config, text, metadata, context);
        double confidence = Optional.ofNullable(metadata.get(OCR_CONFIDENCE)).map(Double::parseDouble).orElse(0.0);
        return new Pass(model, asTesseractText(text.toString()), 100 * confidence);
    }

    // Tesseract's text output ends its last line with a newline, and stored page offsets count it.
    private static String asTesseractText(String text) {
        String stripped = text.strip();
        return stripped.isEmpty() ? "" : stripped + "\n";
    }

    private void run(Path image, TesseractOCRConfig config, ContentHandler handler, Metadata metadata,
                     ParseContext context) throws IOException, SAXException, TikaException {
        context.set(TesseractOCRConfig.class, config);
        try (TikaInputStream input = TikaInputStream.get(image)) {
            delegate.parse(input, handler, metadata, context);
        }
    }

    private static void emit(String text, ContentHandler handler, Metadata metadata) throws SAXException {
        XHTMLContentHandler xhtml = new XHTMLContentHandler(handler, metadata);
        xhtml.startDocument();
        xhtml.startElement("div", "class", "ocr");
        xhtml.characters(text);
        xhtml.endElement("div");
        xhtml.endDocument();
    }

    private record Pass(String model, String text, double confidence) {}
}
