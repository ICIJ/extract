package org.icij.extract.ocr;

import static org.apache.tika.metadata.TikaCoreProperties.TIKA_PARSED_BY;
import static org.fest.assertions.Assertions.assertThat;
import static org.icij.extract.extractor.Extractor.replaceParser;
import static org.icij.extract.ocr.ParserWithConfidence.OCR_CONFIDENCE;
import static org.icij.extract.ocr.Tess4JOCRParser.PSM0_ORIENTATION;
import static org.icij.extract.ocr.Tess4JOCRParser.PSM0_ORIENTATION_CONFIDENCE;
import static org.icij.extract.ocr.Tess4JOCRParser.PSM0_ROTATE;
import static org.icij.extract.ocr.Tess4JOCRParser.PSM0_SCRIPT;
import static org.icij.extract.ocr.Tess4JOCRParser.PSM0_SCRIPT_CONFIDENCE;
import static org.junit.Assert.assertThrows;

import java.io.InputStream;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.tika.config.TikaConfig;
import org.apache.tika.config.TikaTaskTimeout;
import org.apache.tika.exception.TikaConfigException;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.CompositeParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.ocr.TesseractOCRConfig;
import org.apache.tika.parser.ocr.TesseractOCRParser;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.ToXMLContentHandler;
import org.fest.assertions.Delta;
import org.junit.Test;
import org.xml.sax.ContentHandler;

public class Tess4JOCRParserTest {
    private static final String SAMPLE_DOCS_PATH_PREFIX = "/documents/ocr";


    @Test
    public void test_inter_word_spacing() throws Exception {
        // Given
        String path = "test_ocr_spacing.png";
        TesseractOCRConfig withoutSpacingCfg = new TesseractOCRConfig();
        withoutSpacingCfg.setPreserveInterwordSpacing(false);
        ParseContext withoutSpacingCtx = new ParseContext();
        withoutSpacingCtx.set(TesseractOCRConfig.class, withoutSpacingCfg);
        TesseractOCRConfig withSpacingCfg = new TesseractOCRConfig();
        withSpacingCfg.setPreserveInterwordSpacing(true);
        ParseContext withSpacingCtx = new ParseContext();
        withSpacingCtx.set(TesseractOCRConfig.class, withSpacingCfg);
        // When
        String xmlWithSpaces = getXML(path, getMetadata(MediaType.image("png")), withSpacingCtx).xml;
        String xmlWithoutSpaces = getXML(path, getMetadata(MediaType.image("png")), withoutSpacingCtx).xml;
        // Then
        assertThat(xmlWithoutSpaces).contains("The quick");
        Matcher m = Pattern.compile("The\\s{5,20}quick").matcher(xmlWithSpaces);
        assertThat(m.find()).isTrue();
    }

    @Test
    public void test_handle_multipage_tiff() throws Exception {
        // Given
        Metadata meta = getMetadata(MediaType.image("tiff"));
        String docPath = "test_tiff_multipage.tif";
        // When
        String xml = getXML(docPath, meta).xml;
        // Then
        //TIKA-4043 -- on some OS/versions of tesseract Page?2 is extracted
        xml = xml.replaceAll("[^A-Za-z0-9]", " ");
        assertThat(xml).contains("Page 2");
    }

    @Test
    public void test_skip_ocr() throws Exception {
        // Given
        String docPath = "test_tiff_multipage.tif";
        Metadata meta = getMetadata(MediaType.image("tiff"));
        TesseractOCRConfig config = new TesseractOCRConfig();
        config.setSkipOcr(true);
        ParseContext context = new ParseContext();
        context.set(TesseractOCRConfig.class, config);
        // When
        String xml = getXML(docPath, meta, context).xml;
        // Then
        xml = xml.replaceAll("[^A-Za-z0-9]", " ");
        assertThat(xml).doesNotContain("Page 2");
    }

    @Test
    public void test_positive_rotation() throws Exception {
        // Given
        String docPath = "test_rotated+10.png";
        TesseractOCRConfig config = new TesseractOCRConfig();
        config.setApplyRotation(true);
        config.setResize(100);
        ParseContext parseContext = new ParseContext();
        parseContext.set(TesseractOCRConfig.class, config);
        Metadata metadata = getMetadata(MediaType.image("png"));
        // When
        String ocr = getText(docPath, metadata, parseContext);
        // Then
        assertThat(Double.parseDouble(metadata.get(Tess4JOCRParser.IMAGE_ROTATION)))
            .isEqualTo(10., Delta.delta(0.01));
        assertThat(ocr).contains("Its had resolving otherwise she contented therefore");
    }

    @Test
    public void test_negative_rotation() throws Exception {
        // Given
        String docPath = "test_rotated-10.png";
        TesseractOCRConfig config = new TesseractOCRConfig();
        config.setApplyRotation(true);
        config.setResize(100);
        ParseContext parseContext = new ParseContext();
        parseContext.set(TesseractOCRConfig.class, config);
        Metadata metadata = getMetadata(MediaType.image("png"));
        // When
        String ocr = getText(docPath, metadata, parseContext);
        // Then
        assertThat(Double.parseDouble(metadata.get(Tess4JOCRParser.IMAGE_ROTATION)))
            .isEqualTo(-10., Delta.delta(0.01));
        assertThat(ocr).contains("Its had resolving otherwise she contented therefore");
    }

    @Test
    public void test_timeout() throws Exception {
        // Given
        String docPath = "test_rotated+10.png";
        TikaConfig config;
        try (InputStream is = getResourceAsStream("/configs/ocr/TIKA-3582-tesseract.xml")) {
            config = new TikaConfig(is);
        }
        Parser p = new AutoDetectParser(config);
        Metadata m = new Metadata();
        ParseContext parseContext = new ParseContext();
        parseContext.set(TikaTaskTimeout.class, new TikaTaskTimeout(1));
        // When
        assertThat(assertThrows(TikaException.class, () -> getXML(docPath, p, m, parseContext)).getMessage())
            .contains("timeout");
    }

    @Test
    public void test_psm0() throws Exception {
        // Given
        String path = "test_rotated+10.png";
        TikaConfig config;
        try (InputStream is = getResourceAsStream("/configs/ocr/tika-config-psm0.xml")) {
            config = new TikaConfig(is);
        }
        Parser p = new AutoDetectParser(config);
        Metadata m = new Metadata();
        // When
        getXML(path, p, m);
        // Then
        assertThat(m.getInt(PSM0_ORIENTATION)).isEqualTo(180);
        assertThat(m.getInt(PSM0_ROTATE)).isEqualTo(-180);
        assertThat(Double.parseDouble(m.get(PSM0_ORIENTATION_CONFIDENCE))).isEqualTo(5.71, Delta.delta(0.1));
        assertThat(Double.parseDouble(m.get(PSM0_SCRIPT_CONFIDENCE))).isEqualTo(0.83, Delta.delta(0.1));
        assertThat(m.get(PSM0_SCRIPT)).isEqualTo("Latin");
    }

    @Test
    public void test_confidence() throws Exception {
        // Given
        String path = "test_ocr_spacing.png";
        TikaConfig config;
        try (InputStream is = getResourceAsStream("/configs/ocr/TIKA-3582-tesseract.xml")) {
            config = new TikaConfig(is);
        }
        Parser p = new AutoDetectParser(config);
        Metadata m = new Metadata();
        ParseContext parseContext = new ParseContext();
        TesseractOCRConfig ocrConfig = new TesseractOCRConfig();
        ocrConfig.addOtherTesseractConfig("skipConfidence", "true");
        parseContext.set(TesseractOCRConfig.class, ocrConfig);
        // When
        getXML(path, p, m);
        // Then
        Float conf = Optional.ofNullable(m.get(OCR_CONFIDENCE)).map(Float::parseFloat).orElse(0.0f);
        assertThat(conf).isGreaterThan(0.0f);
    }

    @Test
    public void test_hocr() throws Exception {
        // Given
        String path = "test_ocr_spacing.png";
        TikaConfig config;
        try (InputStream is = getResourceAsStream("/configs/ocr/TIKA-3582-tesseract.xml")) {
            config = new TikaConfig(is);
        }
        Parser p = new AutoDetectParser(config);
        Metadata m = new Metadata();
        ParseContext parseContext = new ParseContext();
        TesseractOCRConfig ocrConfig = new TesseractOCRConfig();
        ocrConfig.setOutputType(TesseractOCRConfig.OUTPUT_TYPE.HOCR);
        parseContext.set(TesseractOCRConfig.class, ocrConfig);
        // When
        String ocr = getText(path, p, m, parseContext);
        // Then
        assertThat(ocr).contains("class='ocrx_word'");
    }

    @Test
    public void test_bmp() throws Exception {
        // Given
        String docPath = "test.bmp";
        TesseractOCRConfig config = new TesseractOCRConfig();
        ParseContext parseContext = new ParseContext();
        parseContext.set(TesseractOCRConfig.class, config);
        // When
        String ocr = getText(docPath, parseContext);
        // Then
        String expectedOutput = "The (quick) [brown] {fox} jumps!\nOver the $43,456.78 <lazy> #90 dog";
        assertThat(ocr.substring(0, expectedOutput.length())).isEqualTo(expectedOutput);
    }

    @Test
    public void test_single_image_gif() throws Exception {
        // Given
        String docPath = "test.gif";
        TesseractOCRConfig config = new TesseractOCRConfig();
        ParseContext parseContext = new ParseContext();
        parseContext.set(TesseractOCRConfig.class, config);
        // When
        String ocr = getText(docPath, parseContext);
        // Then
        String expectedOutput = "The (quick) [brown] {fox} jumps!\nOver the $43,456.78 <lazy> #90 dog";
        assertThat(ocr.substring(0, expectedOutput.length())).isEqualTo(expectedOutput);
    }

    @Test
    public void test_jp2() throws Exception {
        // Given
        String docPath = "test.jp2";
        TesseractOCRConfig config = new TesseractOCRConfig();
        ParseContext parseContext = new ParseContext();
        parseContext.set(TesseractOCRConfig.class, config);
        // When
        String ocr = getText(docPath, parseContext);
        // Then
        String expectedOutput = "The (quick) [brown] {fox} jumps";
        assertThat(ocr.substring(0, expectedOutput.length())).isEqualTo(expectedOutput);
    }

    @Test
    public void test_jpeg() throws Exception {
        // Given
        String docPath = "test.jpeg";
        TesseractOCRConfig config = new TesseractOCRConfig();
        ParseContext parseContext = new ParseContext();
        parseContext.set(TesseractOCRConfig.class, config);
        // When
        String ocr = getText(docPath, parseContext);
        // Then
        String expectedOutput = "The (quick) [brown] {fox} jumps!\nOver the $43,456.78 <lazy> #90 dog";
        assertThat(ocr.substring(0, expectedOutput.length())).isEqualTo(expectedOutput);
    }

    @Test
    public void test_jpg() throws Exception {
        // Given
        String docPath = "test.jpg";
        TesseractOCRConfig config = new TesseractOCRConfig();
        ParseContext parseContext = new ParseContext();
        parseContext.set(TesseractOCRConfig.class, config);
        // When
        String ocr = getText(docPath, parseContext);
        // Then
        String expectedOutput = "The (quick) [brown] {fox} jumps!\nOver the $43,456.78 <lazy> #90 dog";
        assertThat(ocr.substring(0, expectedOutput.length())).isEqualTo(expectedOutput);
    }

    @Test
    public void test_jpx() throws Exception {
        // Given
        String docPath = "test.jpx";
        TesseractOCRConfig config = new TesseractOCRConfig();
        ParseContext parseContext = new ParseContext();
        parseContext.set(TesseractOCRConfig.class, config);
        // When
        String ocr = getText(docPath, parseContext);
        // Then
        String expectedOutput = "The (quick) [brown] {fox} jumps!\nOver the $43,456.78 <lazy> #90 dog";
        assertThat(ocr.substring(0, expectedOutput.length())).isEqualTo(expectedOutput);
    }

    @Test
    public void test_png() throws Exception {
        // Given
        String docPath = "test.png";
        TesseractOCRConfig config = new TesseractOCRConfig();
        ParseContext parseContext = new ParseContext();
        parseContext.set(TesseractOCRConfig.class, config);
        // When
        String ocr = getText(docPath, parseContext);
        // Then
        String expectedOutput = "The (quick) [brown] {fox} jumps!\nOver the $43,456.78 <lazy> #90 dog";
        assertThat(ocr.substring(0, expectedOutput.length())).isEqualTo(expectedOutput);
    }

    private String getText(String filePath, ParseContext parseContext) throws Exception {
        return getText(filePath, new Metadata(), parseContext);
    }

    private String getText(String filePath, Metadata metadata, ParseContext parseContext) throws Exception {
        return getText(filePath, null, metadata, parseContext);
    }

    private String getText(String filePath, Parser parser, Metadata metadata, ParseContext parseContext) throws Exception {
        return getText(getResourceAsStream(SAMPLE_DOCS_PATH_PREFIX + "/" + filePath), parser, parseContext, metadata);
    }

    private String getText(InputStream is, Parser parser, ParseContext context, Metadata metadata) throws Exception {
        parser = injectParserIfNeededParser(parser);
        if (context == null) {
            context = new ParseContext();
        }
        ContentHandler handler = new BodyContentHandler(1000000);
        try (is) {
            parser.parse(is, handler, metadata, context);
        }
        assertThat(metadata.getValues(TIKA_PARSED_BY)).contains("org.icij.extract.ocr.Tess4JOCRParser");
        return handler.toString();
    }

    private XMLResult getXML(String filePath, Parser parser, Metadata metadata) throws Exception {
        return getXML(getResourceAsStream(SAMPLE_DOCS_PATH_PREFIX + "/" + filePath), parser, metadata, null);
    }

    private XMLResult getXML(String filePath, Parser parser, Metadata metadata, ParseContext parseContext) throws Exception {
        return getXML(getResourceAsStream(SAMPLE_DOCS_PATH_PREFIX + "/" + filePath), parser, metadata, parseContext);
    }

    private XMLResult getXML(String filePath, Metadata metadata, ParseContext parseContext) throws Exception {
        return getXML(getResourceAsStream(SAMPLE_DOCS_PATH_PREFIX + "/" + filePath), null, metadata, parseContext);
    }

    private XMLResult getXML(String filePath, Metadata metadata) throws Exception {
        return getXML(getResourceAsStream(SAMPLE_DOCS_PATH_PREFIX + "/" + filePath), null, metadata, null);
    }

    private XMLResult getXML(InputStream input, Parser parser, Metadata metadata, ParseContext context) throws Exception {
        parser = injectParserIfNeededParser(parser);
        if (context == null) {
            context = new ParseContext();
        }
        XMLResult xmlResult;
        try (input) {
            ContentHandler handler = new ToXMLContentHandler();
            parser.parse(input, handler, metadata, context);
            xmlResult = new XMLResult(handler.toString(), metadata);
        }
        assertThat(metadata.getValues(TIKA_PARSED_BY))
            .contains("org.icij.extract.ocr.Tess4JOCRParser");
        return xmlResult;
    }

    private static Parser injectParserIfNeededParser(Parser parser) throws TikaConfigException {
        if (parser == null) {
            Tess4JOCRParser newParser = new Tess4JOCRParser();
            newParser.setSkipConfidence(true);
            CompositeParser compositeParser = replaceParser(TikaConfig.getDefaultConfig().getParser(), TesseractOCRParser.class, (ignored) -> newParser);
            parser = new AutoDetectParser(compositeParser);
        }
        Parser ocrParser = findParser(parser, Tess4JOCRParser.class);
        assertThat(ocrParser).isInstanceOf(Tess4JOCRParser.class);
        return parser;
    }

    private static class XMLResult {
        public final String xml;
        public final Metadata metadata;

        public XMLResult(String xml, Metadata metadata) {
            this.xml = xml;
            this.metadata = metadata;
        }
    }

    private Metadata getMetadata(MediaType mediaType) {
        Metadata metadata = new Metadata();
        MediaType ocrMediaType = new MediaType(mediaType.getType(), "OCR-" + mediaType.getSubtype());
        metadata.set(TikaCoreProperties.CONTENT_TYPE_PARSER_OVERRIDE, ocrMediaType.toString());
        return metadata;
    }

    private InputStream getResourceAsStream(String name) {
        InputStream stream = this.getClass().getResourceAsStream(name);
        if (stream == null) {
            throw new RuntimeException("Unable to find requested resource " + name);
        }
        return stream;
    }

    public static Parser findParser(Parser parser, Class<?> clazz) {
        if (parser instanceof CompositeParser) {
            for (Parser child : ((CompositeParser) parser).getAllComponentParsers()) {
                Parser found = findParser(child, clazz);
                if (found != null) {
                    return found;
                }
            }
        } else if (clazz.isInstance(parser)) {
            return parser;
        }
        return null;
    }
}